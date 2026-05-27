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

package au.csiro.pathling.search;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

import au.csiro.pathling.config.AuthorizationConfiguration;
import au.csiro.pathling.config.QueryConfiguration;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.errors.AccessDeniedError;
import au.csiro.pathling.io.source.DataSource;
import ca.uhn.fhir.context.FhirContext;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
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
 * Security tests for {@link SearchProvider} verifying per-resource read authority enforcement.
 *
 * @author John Grimes
 */
@Tag("UnitTest")
@ExtendWith(MockitoExtension.class)
class SearchProviderAuthTest {

  @Mock private ServerConfiguration configuration;
  @Mock private DataSource dataSource;
  @Mock private FhirEncoders fhirEncoders;
  @Mock private Dataset<Row> dataset;

  private SearchProvider searchProvider;
  private FhirContext fhirContext;
  private AuthorizationConfiguration authConfig;

  @BeforeEach
  void setUp() {
    fhirContext = FhirContext.forR4();
    authConfig = new AuthorizationConfiguration();
    when(configuration.getAuth()).thenReturn(authConfig);
    lenient()
        .when(configuration.getQuery())
        .thenReturn(QueryConfiguration.builder().cacheResults(false).build());
    lenient().when(dataSource.read("Patient")).thenReturn(dataset);

    searchProvider =
        new SearchProvider(configuration, fhirContext, dataSource, fhirEncoders, Patient.class);
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
  void searchSucceedsWhenAuthDisabled() {
    authConfig.setEnabled(false);
    assertThat(searchProvider.search(Map.of())).isNotNull();
  }

  @Test
  void searchSucceedsWithCorrectResourceAuthority() {
    authConfig.setEnabled(true);
    setSecurityContext("pathling:search", "pathling:read:Patient");
    assertThat(searchProvider.search(Map.of())).isNotNull();
  }

  @Test
  void searchThrowsWhenMissingResourceAuthority() {
    authConfig.setEnabled(true);
    setSecurityContext("pathling:search");
    assertThatThrownBy(() -> searchProvider.search(Map.of()))
        .isInstanceOf(AccessDeniedError.class)
        .hasMessageContaining("pathling:read:Patient");
  }

  @Test
  void searchWithFiltersSucceedsWithCorrectResourceAuthority() {
    authConfig.setEnabled(true);
    setSecurityContext("pathling:search", "pathling:read:Patient");
    assertThat(searchProvider.search(null, Map.of())).isNotNull();
  }

  @Test
  void searchWithFiltersThrowsWhenMissingResourceAuthority() {
    authConfig.setEnabled(true);
    setSecurityContext("pathling:search");
    assertThatThrownBy(() -> searchProvider.search(null, Map.of()))
        .isInstanceOf(AccessDeniedError.class)
        .hasMessageContaining("pathling:read:Patient");
  }
}
