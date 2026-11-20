/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.csiro.pathling.mcp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import au.csiro.pathling.config.AuthorizationConfiguration;
import au.csiro.pathling.config.ServerConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for the choice of the issuer whose tokens the MCP endpoint accepts.
 *
 * @author John Grimes
 */
class McpSecurityConfigurationTest {

  private ServerConfiguration configuration;

  @BeforeEach
  void setUp() {
    configuration = new ServerConfiguration();
    configuration.setAuth(new AuthorizationConfiguration());
  }

  @Test
  void usesTheMcpIssuerWhenOneIsConfigured() {
    configuration.getAuth().setIssuer("https://fhir-issuer.example.com");
    configuration.getMcp().getAuth().setIssuer("https://mcp-issuer.example.com");

    assertThat(McpSecurityConfiguration.issuer(configuration))
        .isEqualTo("https://mcp-issuer.example.com");
  }

  @Test
  void fallsBackToTheServersIssuer() {
    configuration.getAuth().setIssuer("https://fhir-issuer.example.com");

    assertThat(McpSecurityConfiguration.issuer(configuration))
        .isEqualTo("https://fhir-issuer.example.com");
  }

  @Test
  void refusesToStartWithoutAnyIssuer() {
    assertThatThrownBy(() -> McpSecurityConfiguration.issuer(configuration))
        .isInstanceOf(IllegalStateException.class);
  }
}
