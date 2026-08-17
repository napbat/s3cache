{{- define "s3cache.image" -}}
{{- $repository := required "image.repository is required" .Values.image.repository -}}
{{- $tag := required "image.tag is required" .Values.image.tag -}}
{{- $digest := .Values.image.digest | default "" -}}
{{- if and $digest (not (regexMatch "^sha256:[0-9a-f]{64}$" $digest)) -}}
{{- fail "image.digest must be a sha256 OCI image digest" -}}
{{- end -}}
{{ $repository }}:{{ $tag }}{{ with $digest }}@{{ . }}{{ end }}
{{- end -}}
