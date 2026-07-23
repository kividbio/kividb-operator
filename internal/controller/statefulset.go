package controller

import (
	"fmt"
	"strconv"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func getPort(c *kividbv1alpha1.KividbCluster) int32 {
	if c.Spec.Port == 0 {
		return 6380
	}
	return c.Spec.Port
}

// resolveImage returns spec.image verbatim if set, otherwise
// DefaultKividbImage. The operator never derives or modifies an image
// reference from spec.variant -- variant is informational only (it tells
// the operator whether to wire up TLS/Lua-related configuration), not an
// instruction to pick a different tag. Picking an image that actually
// matches the declared variant is the caller's responsibility; see the
// VariantGuidance/TLSVariantMismatch Events emitted in
// kividbcluster_controller.go for the guidance surfaced back to the user
// on a likely mismatch.
func resolveImage(c *kividbv1alpha1.KividbCluster) string {
	if c.Spec.Image != "" {
		return c.Spec.Image
	}
	return DefaultKividbImage
}

func agentImage(c *kividbv1alpha1.KividbCluster) string {
	if c.Spec.AgentImage != "" {
		return c.Spec.AgentImage
	}
	return DefaultAgentImage
}

func exporterImage(c *kividbv1alpha1.KividbCluster) string {
	if c.Spec.ExporterImage != "" {
		return c.Spec.ExporterImage
	}
	return DefaultExporterImage
}

func gracePeriod(c *kividbv1alpha1.KividbCluster) *int64 {
	if c.Spec.TerminationGracePeriodSeconds != nil {
		return c.Spec.TerminationGracePeriodSeconds
	}
	def := int64(60)
	return &def
}

// defaultAntiAffinity spreads pods of the same cluster across nodes on a
// best-effort basis when the user hasn't set their own Affinity. This is a
// *preferred* rule (not required) so single-node dev/test clusters still
// schedule successfully.
func defaultAntiAffinity(c *kividbv1alpha1.KividbCluster) *corev1.Affinity {
	return &corev1.Affinity{
		PodAntiAffinity: &corev1.PodAntiAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{
				{
					Weight: 100,
					PodAffinityTerm: corev1.PodAffinityTerm{
						LabelSelector: &metav1.LabelSelector{MatchLabels: selectorLabels(c)},
						TopologyKey:   "kubernetes.io/hostname",
					},
				},
			},
		},
	}
}

func agentEnv(c *kividbv1alpha1.KividbCluster, aclConfig *kividbv1alpha1.KividbAclConfig, snapCfg *kividbv1alpha1.KividbSnapshotConfig) []corev1.EnvVar {
	env := []corev1.EnvVar{
		{Name: "KIVIDB_ADDR", Value: fmt.Sprintf("127.0.0.1:%d", getPort(c))},
		{Name: "AGENT_PORT", Value: fmt.Sprintf("%d", AgentPort)},
		{Name: "DATA_DIR", Value: DataDir},
		{Name: "CLUSTER_NAME", Value: c.Name},
		{
			Name: "POD_NAME",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
			},
		},
		{
			Name: "POD_IP",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.podIP"},
			},
		},
	}

	if ref := defaultUserPasswordRef(aclConfig); ref != nil {
		env = append(env, corev1.EnvVar{
			Name: "KIVIDB_AUTH_PASSWORD",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: ref.Name},
					Key:                  ref.Key,
				},
			},
		})
	}

	if snapCfg != nil {
		s3 := snapCfg.Spec.S3
		accessKeyKey := s3.CredentialsSecretRef.AccessKeyIDKey
		if accessKeyKey == "" {
			accessKeyKey = "accessKeyId"
		}
		secretKeyKey := s3.CredentialsSecretRef.SecretAccessKeyKey
		if secretKeyKey == "" {
			secretKeyKey = "secretAccessKey"
		}
		env = append(env,
			corev1.EnvVar{Name: "S3_ENDPOINT", Value: s3.Endpoint},
			corev1.EnvVar{Name: "S3_BUCKET", Value: s3.Bucket},
			corev1.EnvVar{Name: "S3_REGION", Value: s3.Region},
			corev1.EnvVar{Name: "S3_PATH_PREFIX", Value: s3.PathPrefix},
			corev1.EnvVar{Name: "S3_FORCE_PATH_STYLE", Value: fmt.Sprintf("%t", s3.ForcePathStyle)},
			corev1.EnvVar{Name: "S3_INSECURE_SKIP_TLS_VERIFY", Value: fmt.Sprintf("%t", s3.InsecureSkipTLSVerify)},
			corev1.EnvVar{Name: "S3_RETENTION", Value: fmt.Sprintf("%d", snapCfg.Spec.Retention)},
			corev1.EnvVar{
				Name: "S3_ACCESS_KEY_ID",
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: s3.CredentialsSecretRef.Name},
						Key:                  accessKeyKey,
					},
				},
			},
			corev1.EnvVar{
				Name: "S3_SECRET_ACCESS_KEY",
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: s3.CredentialsSecretRef.Name},
						Key:                  secretKeyKey,
					},
				},
			},
		)
	}

	return env
}

// exporterEnv configures oliver006/redis_exporter to talk to the local
// kividb container. redis_exporter reads its target and credentials from
// plain env vars (no CLI flags required), and authenticates the same way
// the agent sidecar does -- via the default ACL user's password, if one
// is configured.
func exporterEnv(c *kividbv1alpha1.KividbCluster, aclConfig *kividbv1alpha1.KividbAclConfig) []corev1.EnvVar {
	env := []corev1.EnvVar{
		{Name: "REDIS_ADDR", Value: fmt.Sprintf("redis://127.0.0.1:%d", getPort(c))},
		{Name: "REDIS_EXPORTER_WEB_LISTEN_ADDRESS", Value: fmt.Sprintf(":%d", ExporterPort)},
	}
	if ref := defaultUserPasswordRef(aclConfig); ref != nil {
		env = append(env, corev1.EnvVar{
			Name: "REDIS_PASSWORD",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: ref.Name},
					Key:                  ref.Key,
				},
			},
		})
	}
	return env
}

func podTemplate(c *kividbv1alpha1.KividbCluster, kdbConfig *kividbv1alpha1.KividbConfig, aclConfig *kividbv1alpha1.KividbAclConfig, snapCfg *kividbv1alpha1.KividbSnapshotConfig) corev1.PodTemplateSpec {
	labels := commonLabels(c)
	for k, v := range c.Spec.PodLabels {
		labels[k] = v
	}
	// RoleLabel starts unset; the controller assigns it once the pod is
	// Ready and a role has been decided (see internal/controller/failover.go).
	// It is intentionally omitted from the template so StatefulSet's
	// pod-template-hash-style reconciliation never fights the controller
	// over it -- the controller patches it directly on the live Pod object.

	affinity := c.Spec.Affinity
	if affinity == nil {
		affinity = defaultAntiAffinity(c)
	}

	port := getPort(c)

	volumeMounts := []corev1.VolumeMount{
		{Name: "data", MountPath: DataDir},
		{Name: "config", MountPath: ConfigDir},
		{Name: "acl", MountPath: AclDir},
	}

	volumes := []corev1.Volume{
		{
			Name: "config",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: configMapName(c)},
				},
			},
		},
		{
			Name: "acl",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{SecretName: secretName(c)},
			},
		},
	}

	kividbArgs := []string{"--configfile", ConfigDir + "/" + ConfigFileName}
	kividbPorts := []corev1.ContainerPort{
		{Name: KividbPortName, ContainerPort: port},
	}

	if tls := kividbTLSSpec(kdbConfig); tls != nil && tls.Enabled {
		certKey := tls.CertSecretRef.CertKey
		if certKey == "" {
			certKey = "tls.crt"
		}
		keyKey := tls.CertSecretRef.KeyKey
		if keyKey == "" {
			keyKey = "tls.key"
		}
		items := []corev1.KeyToPath{
			{Key: certKey, Path: "tls.crt"},
			{Key: keyKey, Path: "tls.key"},
		}
		hasCA := tls.CertSecretRef.CAKey != ""
		if hasCA {
			items = append(items, corev1.KeyToPath{Key: tls.CertSecretRef.CAKey, Path: "ca.crt"})
		}
		volumes = append(volumes, corev1.Volume{
			Name: "tls",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{SecretName: tls.CertSecretRef.Name, Items: items},
			},
		})
		volumeMounts = append(volumeMounts, corev1.VolumeMount{Name: "tls", MountPath: TLSDir, ReadOnly: true})

		tlsPort := tls.Port
		if tlsPort == 0 {
			tlsPort = 6443
		}
		// kividb's --configfile parser does not currently apply tls-port /
		// tls-cert-file / tls-key-file / tls-ca-cert-file directives (a
		// confirmed upstream gap, verified against a live v1.0.2-tls
		// container: the config file is accepted and the process starts,
		// but no TLS listener comes up) -- the identical settings work when
		// passed as CLI flags, so they're passed here in addition to
		// writing them into kividb.conf (which still documents them for
		// anyone reading the rendered ConfigMap directly).
		kividbArgs = append(kividbArgs,
			"--tls-port", strconv.Itoa(int(tlsPort)),
			"--tls-cert-file", TLSDir+"/tls.crt",
			"--tls-key-file", TLSDir+"/tls.key",
		)
		if hasCA {
			kividbArgs = append(kividbArgs, "--tls-ca-cert-file", TLSDir+"/ca.crt")
		}
		kividbPorts = append(kividbPorts, corev1.ContainerPort{Name: "tls", ContainerPort: tlsPort})
	}

	kividbContainer := corev1.Container{
		Name:            "kividb",
		Image:           resolveImage(c),
		ImagePullPolicy: pullPolicyOrDefault(c.Spec.ImagePullPolicy),
		Args:            kividbArgs,
		WorkingDir:      DataDir,
		Ports:           kividbPorts,
		Resources:       c.Spec.Resources,
		VolumeMounts:    volumeMounts,
		LivenessProbe: &corev1.Probe{
			ProbeHandler:        corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt(int(port))}},
			InitialDelaySeconds: 10,
			PeriodSeconds:       10,
			FailureThreshold:    6,
		},
		ReadinessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{Path: "/readyz", Port: intstr.FromInt(AgentPort)},
			},
			InitialDelaySeconds: 5,
			PeriodSeconds:       5,
			FailureThreshold:    3,
		},
	}

	agentContainer := corev1.Container{
		Name:            "agent",
		Image:           agentImage(c),
		ImagePullPolicy: pullPolicyOrDefault(c.Spec.ImagePullPolicy),
		Args:            []string{"serve"},
		Env:             agentEnv(c, aclConfig, snapCfg),
		Resources:       c.Spec.AgentResources,
		Ports: []corev1.ContainerPort{
			{Name: AgentPortName, ContainerPort: AgentPort},
		},
		VolumeMounts: []corev1.VolumeMount{
			{Name: "data", MountPath: DataDir},
		},
		LivenessProbe: &corev1.Probe{
			ProbeHandler:        corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/healthz", Port: intstr.FromInt(AgentPort)}},
			InitialDelaySeconds: 5,
			PeriodSeconds:       10,
		},
	}

	// The readiness probe on the kividb container itself intentionally
	// checks the *agent's* /readyz, since kividb ships no HTTP endpoint of
	// its own; /readyz performs a real RESP PING (and AUTHs first if
	// KIVIDB_AUTH_PASSWORD is set) against 127.0.0.1 from inside the same
	// pod network namespace, so it is equivalent to probing kividb
	// directly.

	containers := []corev1.Container{kividbContainer, agentContainer}
	if c.Spec.Monitoring.Enabled {
		containers = append(containers, corev1.Container{
			Name:            "redis-exporter",
			Image:           exporterImage(c),
			ImagePullPolicy: pullPolicyOrDefault(c.Spec.ImagePullPolicy),
			Env:             exporterEnv(c, aclConfig),
			Resources:       c.Spec.ExporterResources,
			Ports: []corev1.ContainerPort{
				{Name: ExporterPortName, ContainerPort: ExporterPort},
			},
			LivenessProbe: &corev1.Probe{
				ProbeHandler:        corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/health", Port: intstr.FromInt(ExporterPort)}},
				InitialDelaySeconds: 5,
				PeriodSeconds:       15,
			},
		})
	}

	return corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels:      labels,
			Annotations: c.Spec.PodAnnotations,
		},
		Spec: corev1.PodSpec{
			Containers:       containers,
			Volumes:          volumes,
			Tolerations:      c.Spec.Tolerations,
			NodeSelector:     c.Spec.NodeSelector,
			Affinity:         affinity,
			ImagePullSecrets: c.Spec.ImagePullSecrets,
			// kividb's own image runs as a non-root "kividb" user (see its
			// Dockerfile: `useradd -ms /bin/bash kividb`), whose UID is
			// whatever useradd happened to assign -- not something this
			// operator can know or pin. A freshly provisioned PVC is
			// root-owned, so without fsGroup here kividb gets a hard
			// "Permission denied" the first time it tries to SAVE/BGSAVE
			// (confirmed live: `/data/repl_fullsync.kdb.tmp: Permission
			// denied (os error 13)`). Setting fsGroup makes every
			// container in the pod -- including the distroless, UID-65532
			// agent sidecar, which also needs to read these files to
			// upload backups -- a supplementary member of this group,
			// and the volume's group ownership/permissions get set to
			// match on mount, regardless of each container's own UID.
			SecurityContext: &corev1.PodSecurityContext{
				FSGroup: int64Ptr(DataVolumeFSGroup),
			},
			TerminationGracePeriodSeconds: gracePeriod(c),
		},
	}
}

func pullPolicyOrDefault(p corev1.PullPolicy) corev1.PullPolicy {
	if p == "" {
		return corev1.PullIfNotPresent
	}
	return p
}

func desiredStatefulSet(c *kividbv1alpha1.KividbCluster, kdbConfig *kividbv1alpha1.KividbConfig, aclConfig *kividbv1alpha1.KividbAclConfig, snapCfg *kividbv1alpha1.KividbSnapshotConfig) *appsv1.StatefulSet {
	replicas := c.Spec.Replicas + 1 // +1 for the master
	accessModes := c.Spec.Storage.AccessModes
	if len(accessModes) == 0 {
		accessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
	}

	pvc := corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "data", Labels: commonLabels(c)},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      accessModes,
			StorageClassName: c.Spec.Storage.StorageClassName,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse(c.Spec.Storage.Size),
				},
			},
		},
	}

	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      statefulSetName(c),
			Namespace: c.Namespace,
			Labels:    commonLabels(c),
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName:          headlessServiceName(c),
			Replicas:             &replicas,
			PodManagementPolicy:  appsv1.ParallelPodManagement,
			Selector:             &metav1.LabelSelector{MatchLabels: selectorLabels(c)},
			Template:             podTemplate(c, kdbConfig, aclConfig, snapCfg),
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{pvc},
			UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
				Type: appsv1.RollingUpdateStatefulSetStrategyType,
			},
		},
	}
}
