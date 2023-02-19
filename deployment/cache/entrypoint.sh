# Copyright 2023 Commonwealth Scientific and Industrial Research
# Organisation (CSIRO) ABN 41 687 119 230.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e

# Substitute environment variables in the config file, where the ${ENV_VAR} syntax is used.
envs=`printenv`
for env in $envs
do
    IFS== read name value <<< "$env"
    sed -i "s|\${${name}}|${value}|g" /etc/varnish/default.vcl
done

# Run varnishd.
exec varnishd -F \
  -f /etc/varnish/default.vcl \
  -a http=:${VARNISH_HTTP_PORT:-80},HTTP \
  -s ${VARNISH_STORAGE:-malloc}
