{{/* Expand the chart name. */}}
{{- define "messagequeue.name" -}}
{{- default .Chart.Name .Values.global.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Expand a release-scoped resource name. */}}
{{- define "messagequeue.fullname" -}}
{{- if .Values.global.fullnameOverride -}}
{{- .Values.global.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "messagequeue.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{/* Common labels shared by every control-plane object. */}}
{{- define "messagequeue.labels" -}}
helm.sh/chart: {{ include "messagequeue.name" . }}-{{ .Chart.Version | replace "+" "_" }}
app.kubernetes.io/name: {{ include "messagequeue.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
messagequeue.sealos.io/plane: system
{{- end -}}

{{/* Select a service account name while allowing pre-created accounts. */}}
{{- define "messagequeue.controllerServiceAccount" -}}
{{- default (printf "%s-controller" (include "messagequeue.fullname" .)) .Values.serviceAccount.controller.name -}}
{{- end -}}
{{- define "messagequeue.backendServiceAccount" -}}
{{- default (printf "%s-backend" (include "messagequeue.fullname" .)) .Values.serviceAccount.backend.name -}}
{{- end -}}
{{- define "messagequeue.frontendServiceAccount" -}}
{{- default (printf "%s-frontend" (include "messagequeue.fullname" .)) .Values.serviceAccount.frontend.name -}}
{{- end -}}

{{/* Render an image and enforce digests for production values. */}}
{{- define "messagequeue.image" -}}
{{- $root := .root -}}
{{- $image := .image -}}
{{- if not $image.repository }}{{ fail "an image repository is required" }}{{ end -}}
{{- if not $image.tag }}{{ fail "an image tag is required and must not be latest" }}{{ end -}}
{{- if eq $image.tag "latest" }}{{ fail "latest image tags are not allowed" }}{{ end -}}
{{- if and $image.digest (not (regexMatch "^sha256:[a-f0-9]{64}$" $image.digest)) }}{{ fail (printf "invalid sha256 digest for %s" $image.repository) }}{{ end -}}
{{- if and $root.Values.global.imagePolicy.requireDigest (not $image.digest) }}{{ fail (printf "image digest is required for %s" $image.repository) }}{{ end -}}
{{- if $image.digest }}{{ printf "%s@%s" $image.repository $image.digest }}{{ else }}{{ printf "%s:%s" $image.repository $image.tag }}{{ end -}}
{{- end -}}

{{/* Convert a values map into a valid imagePullSecrets list. */}}
{{- define "messagequeue.imagePullSecrets" -}}
{{- with .Values.global.imagePullSecrets }}
imagePullSecrets:
{{- toYaml . | nindent 2 }}
{{- end }}
{{- end -}}
