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

package au.csiro.pathling.operations.create;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

import au.csiro.pathling.config.AuthorizationConfiguration;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.errors.AccessDeniedError;
import au.csiro.pathling.operations.update.UpdateExecutor;
import ca.uhn.fhir.context.FhirContext;
import org.hl7.fhir.r4.model.Patient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.core.authority.AuthorityUtils;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;

/**
 * Security tests for {@link CreateProvider} verifying per-resource write authority enforcement.
 *
 * @author John Grimes
 */
@Tag("UnitTest")
@ExtendWith(MockitoExtension.class)
class CreateProviderAuthTest {

  @Mock private ServerConfiguration configuration;
  @Mock private UpdateExecutor updateExecutor;

  private CreateProvider createProvider;
  private FhirContext fhirContext;
  private AuthorizationConfiguration authConfig;

  @BeforeEach
  void setUp() {
    fhirContext = FhirContext.forR4();
    authConfig = new AuthorizationConfiguration();
    when(configuration.getAuth()).thenReturn(authConfig);
    createProvider = new CreateProvider(configuration, updateExecutor, fhirContext, Patient.class);
  }

  @AfterEach
  void tearDown() {
    SecurityContextHolder.clearContext();
  }

  private void setSecurityContext(final String... authorities) {
    final Jwt jwt = Jwt.withTokenValue("mock").header("alg", "none").claim("sub", "user").build();
    final JwtAuthenticationToken auth =
        new JwtAuthenticationToken(jwt, AuthorityUtils.createAuthorityList(authorities));
    SecurityContextHolder.getContext().setAuthentication(auth);
  }

  @Test
  void createSucceedsWhenAuthDisabled() {
    authConfig.setEnabled(false);
    assertThat(createProvider.create(new Patient())).isNotNull();
  }

  @Test
  void createSucceedsWithCorrectResourceAuthority() {
    authConfig.setEnabled(true);
    setSecurityContext("pathling:create", "pathling:write:Patient");
    assertThat(createProvider.create(new Patient())).isNotNull();
  }

  @Test
  void createThrowsWhenMissingResourceAuthority() {
    authConfig.setEnabled(true);
    setSecurityContext("pathling:create");
    assertThatThrownBy(() -> createProvider.create(new Patient()))
        .isInstanceOf(AccessDeniedError.class)
        .hasMessageContaining("pathling:write:Patient");
  }
}
