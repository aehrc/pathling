#  Copyright 2023 Commonwealth Scientific and Industrial Research
#  Organisation (CSIRO) ABN 41 687 119 230.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from pathling.config import auth_config


def test_builds_default_auth_config(pathling_ctx):
    j_auth_conf = auth_config()
    assert j_auth_conf.isEnabled() is False


def test_builds_non_default_auth_config(pathling_ctx):
    j_auth_conf = auth_config(
        auth_enabled=True,
        auth_use_SMART=False,
        auth_token_endpoint="http://example.com/token",
        auth_client_id="client",
        auth_client_secret="secret",
        auth_private_key_jwk="jwk",
        auth_use_form_for_basic_auth=True,
        auth_scope="scope",
        auth_token_expiry_tolerance=300,
    )
    assert j_auth_conf.isEnabled() is True
    assert j_auth_conf.isUseSMART() is False
    assert j_auth_conf.getTokenEndpoint() == "http://example.com/token"
    assert j_auth_conf.getClientId() == "client"
    assert j_auth_conf.getClientSecret() == "secret"
    assert j_auth_conf.getPrivateKeyJWK() == "jwk"
    assert j_auth_conf.isUseFormForBasicAuth() is True
    assert j_auth_conf.getScope() == "scope"
    assert j_auth_conf.getTokenExpiryTolerance() == 300

