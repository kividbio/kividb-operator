package main

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	kividbv1alpha1 "github.com/kividbio/kividb-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// defaultKividbImage mirrors internal/controller/names.go's
// DefaultKividbImage -- kept in sync manually rather than imported, same
// rationale as the naming helpers below.
const defaultKividbImage = "docker.io/kividb/kividb:v1.1.3"

// Object naming conventions below mirror the frozen convention documented
// in docs/_internal-spec.md and implemented in internal/controller/names.go
// (those helpers are unexported there, so the GUI -- a separate binary --
// re-derives the same names rather than importing controller internals).
func statefulSetName(clusterName string) string    { return clusterName }
func masterServiceName(clusterName string) string  { return clusterName + "-master" }
func replicaServiceName(clusterName string) string { return clusterName + "-replicas" }
func backupCronJobName(clusterName string) string  { return clusterName + "-backup" }

// clusterLabelSelector selects a cluster's kividb pods. The cluster label
// alone is not enough: the operator also puts it on the pods of backup and
// snapshot-restore Jobs, which are not cluster members and run no agent.
func clusterLabelSelector(clusterName string) string {
	return fmt.Sprintf("%s=%s,%s=%s", kividbv1alpha1.ClusterLabel, clusterName, appNameLabel, appNameValue)
}

const (
	appNameLabel = "app.kubernetes.io/name"
	appNameValue = "kividb"

	// shardLabel and serviceKindLabel mirror internal/controller's
	// ShardLabel and ServiceKindLabel.
	shardLabel       = "kividb.io/shard"
	serviceKindLabel = "kividb.io/service"

	// maxShardedNodes mirrors the operator's limit on shards × (replicas+1).
	maxShardedNodes = 24
)

// isClusterPod is clusterLabelSelector for a pod already in hand.
func isClusterPod(pod *corev1.Pod, clusterName string) bool {
	return pod.Labels[kividbv1alpha1.ClusterLabel] == clusterName && pod.Labels[appNameLabel] == appNameValue
}

func portOrDefault(p int32) int32 {
	if p == 0 {
		return 6380
	}
	return p
}

// humanizeAge renders a duration the way `kubectl get` does (coarsest
// meaningful unit, e.g. "3d", "5h", "42s").
func humanizeAge(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd", int(d.Hours()/24))
	}
}

// summarizeCluster builds a ClusterSummary purely from the KividbCluster
// object -- no other API calls -- so the dashboard list view stays cheap
// even with many clusters.
func summarizeCluster(c *kividbv1alpha1.KividbCluster) ClusterSummary {
	ready := 0
	for _, p := range c.Status.Pods {
		if p.Ready {
			ready++
		}
	}

	created := c.CreationTimestamp.Time
	image, imageDefault := c.Spec.Image, false
	if image == "" {
		image, imageDefault = defaultKividbImage, true
	}
	variant := string(c.Spec.Variant)
	if variant == "" {
		variant = string(kividbv1alpha1.VariantStandard)
	}

	// BackupLastSuccess/BackupLastError are deliberately left unset here:
	// they'd require listing KividbSnapshot objects per cluster, which the
	// dashboard's doc comment promises never to do. getClusterDetail fills
	// both in from real KividbSnapshot history.
	return ClusterSummary{
		Namespace:         c.Namespace,
		Name:              c.Name,
		Phase:             string(c.Status.Phase),
		MasterPod:         c.Status.MasterPod,
		Image:             image,
		ImageDefault:      imageDefault,
		Version:           imageTag(image),
		Variant:           variant,
		Replicas:          c.Spec.Replicas,
		Shards:            specShards(c),
		SlotsAssigned:     clusterSlots(c),
		ClusterState:      clusterState(c),
		Rebalancing:       c.Status.Cluster != nil && c.Status.Cluster.Rebalance != nil,
		DesiredPods:       desiredPods(c),
		ReadyPods:         ready,
		TotalPods:         len(c.Status.Pods),
		BackupEnabled:     c.Spec.SnapshotConfigRef != nil,
		CreationTimestamp: created,
		Age:               humanizeAge(time.Since(created)),
	}
}

// desiredPods is the number of pods the cluster should have: the master and
// its replicas, in each shard of a sharded cluster.
func desiredPods(c *kividbv1alpha1.KividbCluster) int32 {
	per := c.Spec.Replicas + 1
	if c.Spec.Shards != nil {
		// While shards are removed, theirs too: they run until drained.
		return shardStatefulSetCount(c) * per
	}
	return per
}

func specShards(c *kividbv1alpha1.KividbCluster) int32 {
	if c.Spec.Shards == nil {
		return 0
	}
	return *c.Spec.Shards
}

func clusterSlots(c *kividbv1alpha1.KividbCluster) int32 {
	if c.Status.Cluster == nil {
		return 0
	}
	return c.Status.Cluster.SlotsAssigned
}

func clusterState(c *kividbv1alpha1.KividbCluster) string {
	if c.Status.Cluster == nil {
		return ""
	}
	return c.Status.Cluster.State
}

// shardStatefulSetCount is how many shard StatefulSets a sharded cluster
// has: spec.shards, or more while shards are being removed.
func shardStatefulSetCount(c *kividbv1alpha1.KividbCluster) int32 {
	n := specShards(c)
	if c.Status.Cluster != nil && c.Status.Cluster.Rebalance != nil {
		n = max(n, c.Status.Cluster.Rebalance.From, c.Status.Cluster.Rebalance.To)
	}
	return n
}

// imageTag is an image reference's tag ("v1.1.2-lua" of
// "docker.io/kividb/kividb:v1.1.3-lua"), without any digest; "" if it has
// none.
func imageTag(image string) string {
	if i := strings.IndexByte(image, '@'); i >= 0 {
		image = image[:i]
	}
	slash := strings.LastIndexByte(image, '/')
	colon := strings.LastIndexByte(image, ':')
	if colon <= slash {
		return ""
	}
	return image[colon+1:]
}

// listClusterSummaries lists KividbClusters (scoped to namespace if
// non-empty, all namespaces otherwise) and returns dashboard rows sorted by
// namespace then name for stable rendering.
func listClusterSummaries(ctx context.Context, ctrlClient client.Client, namespace string) ([]ClusterSummary, error) {
	var list kividbv1alpha1.KividbClusterList
	var opts []client.ListOption
	if namespace != "" {
		opts = append(opts, client.InNamespace(namespace))
	}
	if err := ctrlClient.List(ctx, &list, opts...); err != nil {
		return nil, fmt.Errorf("listing kividbclusters: %w", err)
	}

	out := make([]ClusterSummary, 0, len(list.Items))
	for i := range list.Items {
		out = append(out, summarizeCluster(&list.Items[i]))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Namespace != out[j].Namespace {
			return out[i].Namespace < out[j].Namespace
		}
		return out[i].Name < out[j].Name
	})
	return out, nil
}

// getClusterDetail fetches the full detail payload for one cluster. The
// KividbCluster object itself is authoritative and required; enrichment
// from Pods/StatefulSet/Services/CronJob/Events is best-effort -- a failure
// to fetch any of those (e.g. the object doesn't exist, or a watch-only
// RBAC edge case) is logged by the caller but never fails the whole page,
// since the CRD's own status already carries the essentials.
func getClusterDetail(ctx context.Context, ctrlClient client.Client, clientset kubernetes.Interface, namespace, name string) (*ClusterDetail, error) {
	var c kividbv1alpha1.KividbCluster
	if err := ctrlClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &c); err != nil {
		return nil, err
	}

	det := &ClusterDetail{
		ClusterSummary:     summarizeCluster(&c),
		AgentImage:         c.Spec.AgentImage,
		Port:               portOrDefault(c.Spec.Port),
		StorageSize:        c.Spec.Storage.Size,
		MasterServiceType:  string(c.Spec.Services.Master.Type),
		ReplicaServiceType: string(c.Spec.Services.Replicas.Type),
		ObservedGeneration: c.Status.ObservedGeneration,
		Cluster:            c.Status.Cluster,
		ShardStatus:        c.Status.Shards,
	}
	if c.Spec.ConfigRef != nil {
		det.ConfigRef = c.Spec.ConfigRef.Name
	}
	if c.Spec.AclConfigRef != nil {
		det.AclConfigRef = c.Spec.AclConfigRef.Name
	}
	if c.Spec.Storage.StorageClassName != nil {
		det.StorageClassName = *c.Spec.Storage.StorageClassName
	}
	if c.Spec.SnapshotConfigRef != nil {
		det.SnapshotConfigRef = c.Spec.SnapshotConfigRef.Name
		var snapCfg kividbv1alpha1.KividbSnapshotConfig
		if err := ctrlClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: c.Spec.SnapshotConfigRef.Name}, &snapCfg); err == nil {
			det.BackupSchedule = snapCfg.Spec.Schedule
			det.BackupRetention = snapCfg.Spec.Retention
		}
		det.Snapshots = fetchSnapshots(ctx, ctrlClient, namespace, name)
		for _, s := range det.Snapshots {
			switch s.Phase {
			case string(kividbv1alpha1.SnapshotSucceeded):
				if s.CompletionTime != nil && (det.BackupLastSuccess == nil || s.CompletionTime.After(*det.BackupLastSuccess)) {
					det.BackupLastSuccess = s.CompletionTime
				}
			case string(kividbv1alpha1.SnapshotFailed):
				if det.BackupLastError == "" {
					det.BackupLastError = s.Error
				}
			}
		}
	}
	if c.Status.LastFailoverTime != nil {
		t := c.Status.LastFailoverTime.Time
		det.LastFailoverTime = &t
	}
	for _, cond := range c.Status.Conditions {
		det.Conditions = append(det.Conditions, ConditionView{
			Type:    cond.Type,
			Status:  string(cond.Status),
			Reason:  cond.Reason,
			Message: cond.Message,
		})
	}

	podByName := make(map[string]int, len(c.Status.Pods))
	for _, p := range c.Status.Pods {
		det.Pods = append(det.Pods, PodView{
			Name:              p.Name,
			Role:              string(p.Role),
			Ready:             p.Ready,
			ReplicationOffset: p.ReplicationOffset,
			Shard:             p.Shard,
			NodeID:            p.NodeID,
		})
		podByName[p.Name] = len(det.Pods) - 1
	}

	if clientset == nil {
		return det, nil
	}

	enrichPods(ctx, clientset, namespace, name, det, podByName)
	if c.Spec.Shards != nil {
		det.StatefulSet = fetchShardStatefulSetsView(ctx, clientset, namespace, name, shardStatefulSetCount(&c))
		det.Services = fetchShardedServiceViews(ctx, clientset, namespace, name)
	} else {
		det.StatefulSet = fetchStatefulSetView(ctx, clientset, namespace, statefulSetName(name))
		det.Services = fetchServiceViews(ctx, clientset, namespace, name)
	}
	if c.Spec.SnapshotConfigRef != nil {
		det.CronJob = fetchCronJobView(ctx, clientset, namespace, backupCronJobName(name))
	}
	det.Events = fetchClusterEvents(ctx, clientset, namespace, name)

	return det, nil
}

// enrichPods overlays live Pod status (phase, IP, node, restarts) onto the
// PodViews already populated from KividbCluster.status.pods[]. Pods listed
// live but absent from status.pods[] (e.g. brand new, not yet observed by
// the controller) are appended too.
func enrichPods(ctx context.Context, clientset kubernetes.Interface, namespace, clusterName string, det *ClusterDetail, podByName map[string]int) {
	pods, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: clusterLabelSelector(clusterName),
	})
	if err != nil {
		return
	}

	for _, p := range pods.Items {
		var restarts int32
		for _, cs := range p.Status.ContainerStatuses {
			restarts += cs.RestartCount
		}

		idx, ok := podByName[p.Name]
		if !ok {
			det.Pods = append(det.Pods, PodView{Name: p.Name})
			idx = len(det.Pods) - 1
			podByName[p.Name] = idx
		}
		if det.Pods[idx].Shard == nil {
			if s, err := strconv.Atoi(p.Labels[shardLabel]); err == nil {
				shard := int32(s)
				det.Pods[idx].Shard = &shard
			}
		}
		det.Pods[idx].Phase = string(p.Status.Phase)
		det.Pods[idx].PodIP = p.Status.PodIP
		det.Pods[idx].NodeName = p.Spec.NodeName
		det.Pods[idx].RestartCount = restarts
	}

	sort.Slice(det.Pods, func(i, j int) bool { return det.Pods[i].Name < det.Pods[j].Name })
}

func fetchStatefulSetView(ctx context.Context, clientset kubernetes.Interface, namespace, name string) *StatefulSetView {
	sts, err := clientset.AppsV1().StatefulSets(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil
	}
	var desired int32
	if sts.Spec.Replicas != nil {
		desired = *sts.Spec.Replicas
	}
	return &StatefulSetView{
		DesiredReplicas: desired,
		ReadyReplicas:   sts.Status.ReadyReplicas,
		CurrentReplicas: sts.Status.CurrentReplicas,
		UpdatedReplicas: sts.Status.UpdatedReplicas,
	}
}

// fetchShardStatefulSetsView adds up a sharded cluster's StatefulSets,
// <cluster>-<shard>.
func fetchShardStatefulSetsView(ctx context.Context, clientset kubernetes.Interface, namespace, clusterName string, shards int32) *StatefulSetView {
	var sum *StatefulSetView
	for s := int32(0); s < shards; s++ {
		v := fetchStatefulSetView(ctx, clientset, namespace, fmt.Sprintf("%s-%d", clusterName, s))
		if v == nil {
			continue
		}
		if sum == nil {
			sum = &StatefulSetView{}
		}
		sum.DesiredReplicas += v.DesiredReplicas
		sum.ReadyReplicas += v.ReadyReplicas
		sum.CurrentReplicas += v.CurrentReplicas
		sum.UpdatedReplicas += v.UpdatedReplicas
	}
	return sum
}

// fetchShardedServiceViews lists a sharded cluster's Services: the whole
// cluster's, the replicas', each shard's and each shard's headless one.
// The one per pod is left out (there are as many as pods), as are the
// Services users make for access from outside.
func fetchShardedServiceViews(ctx context.Context, clientset kubernetes.Interface, namespace, clusterName string) []ServiceView {
	list, err := clientset.CoreV1().Services(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=%s,%s,%s!=pod", kividbv1alpha1.ClusterLabel, clusterName, serviceKindLabel, serviceKindLabel),
	})
	if err != nil {
		return nil
	}
	order := map[string]int{"cluster": 0, "replicas": 1, "shard": 2, "headless": 3}
	var views []ServiceView
	for i := range list.Items {
		svc := &list.Items[i]
		sv := ServiceView{Name: svc.Name, Kind: svc.Labels[serviceKindLabel], Type: string(svc.Spec.Type), ClusterIP: svc.Spec.ClusterIP}
		for _, p := range svc.Spec.Ports {
			sv.Ports = append(sv.Ports, p.Port)
		}
		views = append(views, sv)
	}
	sort.Slice(views, func(i, j int) bool {
		if order[views[i].Kind] != order[views[j].Kind] {
			return order[views[i].Kind] < order[views[j].Kind]
		}
		return views[i].Name < views[j].Name
	})
	return views
}

func fetchServiceViews(ctx context.Context, clientset kubernetes.Interface, namespace, clusterName string) []ServiceView {
	names := []string{masterServiceName(clusterName), replicaServiceName(clusterName)}
	var views []ServiceView
	for _, svcName := range names {
		svc, err := clientset.CoreV1().Services(namespace).Get(ctx, svcName, metav1.GetOptions{})
		if err != nil {
			continue
		}
		sv := ServiceView{
			Name:      svc.Name,
			Type:      string(svc.Spec.Type),
			ClusterIP: svc.Spec.ClusterIP,
		}
		for _, p := range svc.Spec.Ports {
			sv.Ports = append(sv.Ports, p.Port)
		}
		if svc.Spec.Type == corev1.ServiceTypeLoadBalancer {
			for _, ing := range svc.Status.LoadBalancer.Ingress {
				if ing.IP != "" {
					sv.ExternalIP = ing.IP
					break
				}
				if ing.Hostname != "" {
					sv.ExternalIP = ing.Hostname
					break
				}
			}
		}
		views = append(views, sv)
	}
	return views
}

func fetchCronJobView(ctx context.Context, clientset kubernetes.Interface, namespace, name string) *CronJobView {
	cj, err := clientset.BatchV1().CronJobs(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil
	}
	v := &CronJobView{
		Name:      cj.Name,
		Schedule:  cj.Spec.Schedule,
		Suspended: cj.Spec.Suspend != nil && *cj.Spec.Suspend,
	}
	if cj.Status.LastScheduleTime != nil {
		t := cj.Status.LastScheduleTime.Time
		v.LastScheduleTime = &t
	}
	if cj.Status.LastSuccessfulTime != nil {
		t := cj.Status.LastSuccessfulTime.Time
		v.LastSuccessfulTime = &t
	}
	return v
}

// fetchSnapshots lists the KividbSnapshot records belonging to one
// cluster (via the operator-set kividb.io/cluster label), newest first,
// capped to a reasonable page for the detail view.
func fetchSnapshots(ctx context.Context, ctrlClient client.Client, namespace, clusterName string) []SnapshotView {
	const maxSnapshots = 20

	var list kividbv1alpha1.KividbSnapshotList
	if err := ctrlClient.List(ctx, &list, client.InNamespace(namespace), client.MatchingLabels{kividbv1alpha1.ClusterLabel: clusterName}); err != nil {
		return nil
	}

	views := make([]SnapshotView, 0, len(list.Items))
	for _, s := range list.Items {
		v := SnapshotView{
			Name:       s.Name,
			Phase:      string(s.Status.Phase),
			SourcePod:  s.Status.SourcePod,
			SourceRole: string(s.Status.SourceRole),
			ObjectKey:  s.Status.ObjectKey,
			SizeBytes:  s.Status.SizeBytes,
			DurationMs: s.Status.DurationMs,
			Error:      s.Status.Error,
			Shards:     s.Status.Shards,
		}
		if s.Status.StartTime != nil {
			t := s.Status.StartTime.Time
			v.StartTime = &t
		}
		if s.Status.CompletionTime != nil {
			t := s.Status.CompletionTime.Time
			v.CompletionTime = &t
		}
		views = append(views, v)
	}

	sort.Slice(views, func(i, j int) bool {
		ti, tj := snapshotSortTime(views[i]), snapshotSortTime(views[j])
		return tj.Before(ti)
	})
	if len(views) > maxSnapshots {
		views = views[:maxSnapshots]
	}
	return views
}

// snapshotSortTime picks the best available timestamp for ordering: a
// completed snapshot sorts by completion, an in-progress one by start.
func snapshotSortTime(v SnapshotView) time.Time {
	if v.CompletionTime != nil {
		return *v.CompletionTime
	}
	if v.StartTime != nil {
		return *v.StartTime
	}
	return time.Time{}
}

// fetchClusterEvents lists Events involving the KividbCluster object
// itself (not its Pods/StatefulSet/etc.), filtered server-side via the
// Events API's field selector on involvedObject. Returned newest-first,
// capped to a reasonable page for the detail view.
func fetchClusterEvents(ctx context.Context, clientset kubernetes.Interface, namespace, name string) []EventView {
	const maxEvents = 50

	selector := fmt.Sprintf("involvedObject.kind=KividbCluster,involvedObject.name=%s", name)
	list, err := clientset.CoreV1().Events(namespace).List(ctx, metav1.ListOptions{FieldSelector: selector})
	if err != nil {
		return nil
	}

	events := make([]EventView, 0, len(list.Items))
	for _, e := range list.Items {
		source := e.Source.Component
		if e.ReportingController != "" {
			source = e.ReportingController
		}
		ev := EventView{
			Type:    e.Type,
			Reason:  e.Reason,
			Message: e.Message,
			Count:   e.Count,
			Source:  source,
		}
		if !e.FirstTimestamp.IsZero() {
			ev.FirstTimestamp = e.FirstTimestamp.Time
		}
		switch {
		case !e.LastTimestamp.IsZero():
			ev.LastTimestamp = e.LastTimestamp.Time
		case !e.EventTime.IsZero():
			// Some event producers (server-side-apply / events.k8s.io style)
			// only set EventTime rather than the legacy FirstTimestamp/
			// LastTimestamp pair; core/v1 Events surface both fields.
			ev.LastTimestamp = e.EventTime.Time
		}
		events = append(events, ev)
	}

	sort.Slice(events, func(i, j int) bool { return events[i].LastTimestamp.After(events[j].LastTimestamp) })
	if len(events) > maxEvents {
		events = events[:maxEvents]
	}
	return events
}
