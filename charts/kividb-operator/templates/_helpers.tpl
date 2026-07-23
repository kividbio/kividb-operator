{{/*
Base chart name. Deliberately a literal, not .Chart.Name: the OCI push
path for this chart (see .github/workflows/release.yaml) needs its own
Chart.yaml "name" distinct from "kividb-operator" (to avoid colliding
with the quay.io/kividbio/kividb-operator *image* repo at the same path),
and that must never ripple into the actual deployed object names/labels
below, which have always been "kividb-operator" and should stay that way
regardless of what the chart's own package name is.
*/}}
{{- define "kividb-operator.name" -}}
{{- default "kividb-operator" .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Fully qualified app name, used as a prefix for most object names.
*/}}
{{- define "kividb-operator.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default "kividb-operator" .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Chart name and version, for the helm.sh/chart label.
*/}}
{{- define "kividb-operator.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Common labels applied to every object this chart renders.
*/}}
{{- define "kividb-operator.labels" -}}
helm.sh/chart: {{ include "kividb-operator.chart" . }}
{{ include "kividb-operator.selectorLabels" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
{{- end -}}

{{/*
Selector labels for the manager Deployment.
*/}}
{{- define "kividb-operator.selectorLabels" -}}
app.kubernetes.io/name: {{ include "kividb-operator.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
Name of the manager ServiceAccount to use.
*/}}
{{- define "kividb-operator.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (printf "%s-manager" (include "kividb-operator.fullname" .)) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/*
Name of the manager ClusterRole (kept as a fixed, predictable name so it
matches config/rbac/role.yaml exactly regardless of release name).
*/}}
{{- define "kividb-operator.clusterRoleName" -}}
kividb-operator-manager-role
{{- end -}}

{{/*
GUI component name/labels.
*/}}
{{- define "kividb-operator.gui.fullname" -}}
{{- printf "%s-gui" (include "kividb-operator.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "kividb-operator.gui.selectorLabels" -}}
app.kubernetes.io/name: {{ include "kividb-operator.name" . }}-gui
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "kividb-operator.gui.labels" -}}
helm.sh/chart: {{ include "kividb-operator.chart" . }}
{{ include "kividb-operator.gui.selectorLabels" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
{{- end -}}

{{/*
Name of the GUI's own ServiceAccount. Always its own dedicated SA,
independent of the manager's serviceAccount.create/name settings -- see
docs/_internal-spec.md: the GUI must never run under the manager's
ClusterRole.
*/}}
{{- define "kividb-operator.gui.serviceAccountName" -}}
{{- printf "%s-gui" (include "kividb-operator.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
