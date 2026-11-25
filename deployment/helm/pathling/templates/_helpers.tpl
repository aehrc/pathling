{{/*
Copyright © 2026, Commonwealth Scientific and Industrial Research Organisation (CSIRO)
ABN 41 687 119 230. Licensed under the Apache License, Version 2.0.

Author: John Grimes
*/}}

{{/*
Looks up a configuration value across pathling.config and pathling.secretConfig, returning it as a
string, or the supplied default when neither sets it. A null map or value counts as unset. Takes a
list of the root context, the key and the default.
*/}}
{{- define "pathling.configValue" -}}
{{- $root := index . 0 -}}
{{- $key := index . 1 -}}
{{- $value := index . 2 -}}
{{- range list $root.Values.pathling.secretConfig $root.Values.pathling.config -}}
{{- $configValue := index (. | default dict) $key -}}
{{- if not (kindIs "invalid" $configValue) -}}
{{- $value = $configValue -}}
{{- end -}}
{{- end -}}
{{- $value | toString -}}
{{- end -}}

{{/*
Renders "true" when Spark is configured with a cluster manager, so that executors outside the server
pod need to reach the driver. The server defaults to a local master.
*/}}
{{- define "pathling.clusterMode" -}}
{{- if not (hasPrefix "local" (include "pathling.configValue" (list . "spark.master" "local[*]"))) -}}
true
{{- end -}}
{{- end -}}

{{/*
Renders "true" when the Spark UI is enabled. Spark runs the UI unless spark.ui.enabled is false.
*/}}
{{- define "pathling.sparkUiEnabled" -}}
{{- if ne (lower (include "pathling.configValue" (list . "spark.ui.enabled" "true"))) "false" -}}
true
{{- end -}}
{{- end -}}
