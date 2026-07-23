{{- define "sceptre.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "sceptre.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := include "sceptre.name" . -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "sceptre.labels" -}}
app.kubernetes.io/name: {{ include "sceptre.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | quote }}
{{- end -}}

{{- define "sceptre.selectorLabels" -}}
app.kubernetes.io/name: {{ include "sceptre.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "sceptre.image" -}}
{{- $root := .root -}}
{{- $image := .image -}}
{{- $repository := $image.repository -}}
{{- $useGlobal := true -}}
{{- if hasKey $image "useGlobal" -}}
{{- $useGlobal = $image.useGlobal -}}
{{- end -}}
{{- if and $root.Values.global.imageRegistry $useGlobal -}}
{{- $repository = printf "%s/%s" (trimSuffix "/" $root.Values.global.imageRegistry) $repository -}}
{{- end -}}
{{- if $image.digest -}}
{{- printf "%s@%s" $repository $image.digest -}}
{{- else -}}
{{- $tag := $image.tag -}}
{{- if and (not $tag) $image.tagPrefix -}}
{{- $tag = printf "%s-%s" $image.tagPrefix $root.Chart.AppVersion -}}
{{- end -}}
{{- printf "%s:%s" $repository (default $root.Chart.AppVersion $tag) -}}
{{- end -}}
{{- end -}}

{{- define "sceptre.platformSecretName" -}}
{{- default (printf "%s-platform" (include "sceptre.fullname" .)) .Values.platform.existingSecret -}}
{{- end -}}

{{- define "sceptre.objectStoreSecretName" -}}
{{- if .Values.seaweedfs.enabled -}}
{{- default (printf "%s-seaweedfs" (include "sceptre.fullname" .)) .Values.seaweedfs.auth.existingSecret -}}
{{- else -}}
{{- required "externalObjectStore.existingSecret is required when seaweedfs.enabled=false" .Values.externalObjectStore.existingSecret -}}
{{- end -}}
{{- end -}}

{{- define "sceptre.objectStoreAccessKey" -}}
{{- ternary .Values.seaweedfs.auth.accessKeyKey .Values.externalObjectStore.accessKeyKey .Values.seaweedfs.enabled -}}
{{- end -}}

{{- define "sceptre.objectStoreSecretKey" -}}
{{- ternary .Values.seaweedfs.auth.secretKeyKey .Values.externalObjectStore.secretKeyKey .Values.seaweedfs.enabled -}}
{{- end -}}

{{- define "sceptre.objectStoreEndpoint" -}}
{{- if .Values.seaweedfs.enabled -}}
{{- printf "http://%s-seaweedfs:8333" (include "sceptre.fullname" .) -}}
{{- else -}}
{{- required "externalObjectStore.endpoint is required when seaweedfs.enabled=false" .Values.externalObjectStore.endpoint -}}
{{- end -}}
{{- end -}}

{{- define "sceptre.objectStoreBucket" -}}
{{- ternary .Values.seaweedfs.bucket .Values.externalObjectStore.bucket .Values.seaweedfs.enabled -}}
{{- end -}}

{{- define "sceptre.mlflowTrackingUri" -}}
{{- if .Values.mlflow.enabled -}}
{{- printf "http://%s-mlflow:5000" (include "sceptre.fullname" .) -}}
{{- else -}}
{{- required "mlflow.externalTrackingUri is required when mlflow.enabled=false" .Values.mlflow.externalTrackingUri -}}
{{- end -}}
{{- end -}}

{{- define "sceptre.trainingServiceAccount" -}}
{{- printf "%s-training" (include "sceptre.fullname" .) -}}
{{- end -}}

{{- define "sceptre.inferenceServiceAccount" -}}
{{- printf "%s-inference" (include "sceptre.fullname" .) -}}
{{- end -}}

{{- define "sceptre.apiServiceAccount" -}}
{{- printf "%s-api" (include "sceptre.fullname" .) -}}
{{- end -}}

{{- define "sceptre.authSecretName" -}}
{{- default (printf "%s-auth" (include "sceptre.fullname" .)) .Values.auth.existingSecret -}}
{{- end -}}

{{- define "sceptre.databaseUrl" -}}
{{- if .Values.postgresql.enabled -}}
{{- printf "postgresql+psycopg://%s:%s@%s-postgresql:5432/%s" .Values.postgresql.auth.username .Values.postgresql.auth.password (include "sceptre.fullname" .) .Values.postgresql.auth.database -}}
{{- else -}}
{{- required "externalDatabase.url is required when postgresql.enabled=false" .Values.externalDatabase.url -}}
{{- end -}}
{{- end -}}

{{- define "sceptre.mlflowDatabaseUrl" -}}
{{- if .Values.postgresql.enabled -}}
{{- printf "postgresql+psycopg2://%s:%s@%s-postgresql:5432/mlflow" .Values.postgresql.auth.username .Values.postgresql.auth.password (include "sceptre.fullname" .) -}}
{{- else if .Values.mlflow.enabled -}}
{{- required "externalDatabase.mlflowUrl is required for bundled MLflow when postgresql.enabled=false" .Values.externalDatabase.mlflowUrl -}}
{{- else -}}
{{- .Values.externalDatabase.mlflowUrl -}}
{{- end -}}
{{- end -}}
