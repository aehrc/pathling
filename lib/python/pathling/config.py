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
from typing import Optional

from pathling.jvm import jvm_pathling
from py4j.java_gateway import JVMView, JavaObject


def auth_config(
    auth_enabled: bool = False,
    auth_token_endpoint: Optional[str] = None,
    auth_client_id: Optional[str] = None,
    auth_client_secret: Optional[str] = None,
    auth_scope: Optional[str] = None,
    auth_token_expiry_tolerance: int = 120,
) -> JavaObject:
    """
    Creates authentication configuration.

    :param auth_enabled: enables authentication of requests to the fhir endpoint server
    :param auth_token_endpoint: an OAuth2 token endpoint for use with the client credentials grant
    :param auth_client_id: a client ID for use with the client credentials grant
    :param auth_client_secret: a client secret for use with the client credentials grant
    :param auth_scope: a scope value for use with the client credentials grant
    :param auth_token_expiry_tolerance: the minimum number of seconds that a token should have
           before expiry when deciding whether to send it with a terminology request
    :return:  configured java instance of AuthConfiguration
    """
    return (
        jvm_pathling()
        .config.AuthConfiguration.builder()
        .enabled(auth_enabled)
        .tokenEndpoint(auth_token_endpoint)
        .clientId(auth_client_id)
        .clientSecret(auth_client_secret)
        .scope(auth_scope)
        .tokenExpiryTolerance(auth_token_expiry_tolerance)
        .build()
    )
