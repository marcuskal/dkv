{{/* Name of the chart, honoring nameOverride */}}
{{- define "dkv.name" -}}
{{- if .Values.nameOverride }}
{{- .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- .Chart.Name | trunc 63 | trimSuffix "-" }}
{{- end -}}
{{- end }}

{{/* Fullname includes release name unless fullnameOverride is set */}}
{{- define "dkv.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := include "dkv.name" . -}}
{{- printf "%s-%s" $name .Release.Name | trunc 63 | trimSuffix "-" }}
{{- end -}}
{{- end }}

{{/* Common labels */}}
{{- define "dkv.labels" -}}
app.kubernetes.io/name: {{ include "dkv.name" . }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/* Selector labels used by Deployment/StatefulSet selectors */}}
{{- define "dkv.selectorLabels" -}}
app.kubernetes.io/name: {{ include "dkv.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/* Headless service name used by the StatefulSet for peer DNS */}}
{{- define "dkv.headlessServiceName" -}}
{{- printf "%s-headless" (include "dkv.fullname" .) -}}
{{- end }}

{{/* Service account name */}}
{{- define "dkv.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
  {{- default (include "dkv.fullname" .) .Values.serviceAccount.name }}
{{- else }}
  {{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/* Kubernetes cluster domain — override via --set clusterDomain=... if yours differs */}}
{{- define "dkv.clusterDomain" -}}
{{- default "cluster.local" .Values.clusterDomain -}}
{{- end }}
