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

package au.csiro.pathling.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import au.csiro.pathling.errors.AccessDeniedError;
import au.csiro.pathling.read.ReadExecutor;
import au.csiro.pathling.read.ReadProvider;
import au.csiro.pathling.read.ReadProviderFactory;
import au.csiro.pathling.util.FhirServerTestConfiguration;
import jakarta.annotation.Nonnull;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.IdType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

/**
 * Security tests for the FHIR read interaction. Verifies that both checks that guard the
 * interaction fire together: the operation authority check applied by {@link SecurityAspect} before
 * method entry, and the per-resource-type data authority check applied inline by the provider.
 *
 * <p>Providers are obtained from {@link ReadProviderFactory}, which is the path {@code FhirServer}
 * uses to register them. Exercising that path is what makes the operation check observable: the
 * check is applied by AspectJ {@code @Before} advice that only fires when the provider is a Spring
 * bean, so a test that constructs the provider directly cannot see it.
 *
 * @author John Grimes
 */
@TestPropertySource(
    properties = {
      "pathling.auth.enabled=true",
      "pathling.auth.issuer=https://pathling.acme.com/fhir"
    })
@MockitoBean(types = OidcConfiguration.class)
@MockitoBean(types = JwtDecoder.class)
@MockitoBean(types = JwtAuthenticationConverter.class)
@Import({FhirServerTestConfiguration.class, ReadExecutor.class, ReadProviderFactory.class})
class SecurityEnabledReadOperationTest extends SecurityTest {

  private static final String ERROR_MSG_TEMPLATE = "Missing authority: 'pathling:%s'";

  /** An identifier present in the Patient delta table backing the test data source. */
  private static final String PATIENT_ID = "72df0f76-2758-fac4-67cd-de33c4a2c95e";

  /** An identifier present in the Observation delta table backing the test data source. */
  private static final String OBSERVATION_ID = "88d6aa70-4187-2360-9da6-3113decd1c21";

  @Autowired private ReadProviderFactory readProviderFactory;

  // -------------------------------------------------------------------------
  // User story 1: narrow per-type instance read.
  // -------------------------------------------------------------------------

  @Test
  @DisplayName("US1: read-resource plus read:Observation reads an Observation by id")
  @WithMockJwt(
      username = "user",
      authorities = {"pathling:read-resource", "pathling:read:Observation"})
  void readAllowedWithOperationAuthorityAndMatchingTypeAuthority() {
    final IBaseResource resource = read("Observation", OBSERVATION_ID);

    assertThat(resource).isNotNull();
    assertThat(resource.getIdElement().getIdPart()).isEqualTo(OBSERVATION_ID);
  }

  @Test
  @DisplayName("US1: read-resource plus read:Observation is refused a Patient read")
  @WithMockJwt(
      username = "user",
      authorities = {"pathling:read-resource", "pathling:read:Observation"})
  void readDeniedWithOperationAuthorityButWrongTypeAuthority() {
    // The operation check passes and the inline data check refuses, naming the type it wanted.
    assertThatThrownBy(() -> read("Patient", PATIENT_ID))
        .isExactlyInstanceOf(AccessDeniedError.class)
        .hasMessage(ERROR_MSG_TEMPLATE.formatted("read:Patient"));
  }

  @Test
  @DisplayName("US1: read:Observation alone is refused, naming the missing operation authority")
  @WithMockJwt(
      username = "user",
      authorities = {"pathling:read:Observation"})
  void readDeniedWithoutOperationAuthority() {
    assertThatThrownBy(() -> read("Observation", OBSERVATION_ID))
        .isExactlyInstanceOf(AccessDeniedError.class)
        .hasMessage(ERROR_MSG_TEMPLATE.formatted("read-resource"));
  }

  @Test
  @DisplayName("US1: read-resource plus all-types read reads any type by id")
  @WithMockJwt(
      username = "user",
      authorities = {"pathling:read-resource", "pathling:read"})
  void readAllowedWithOperationAuthorityAndAllTypesDataAuthority() {
    assertThat(read("Observation", OBSERVATION_ID).getIdElement().getIdPart())
        .isEqualTo(OBSERVATION_ID);
    assertThat(read("Patient", PATIENT_ID).getIdElement().getIdPart()).isEqualTo(PATIENT_ID);
  }

  // -------------------------------------------------------------------------
  // User story 2: the data authority no longer grants the operation.
  // -------------------------------------------------------------------------

  @Test
  @DisplayName("US2: the all-types read data authority alone no longer permits the interaction")
  @WithMockJwt(
      username = "user",
      authorities = {"pathling:read"})
  void readDeniedWithDataAuthorityOnly() {
    assertThatThrownBy(() -> read("Patient", PATIENT_ID))
        .isExactlyInstanceOf(AccessDeniedError.class)
        .hasMessage(ERROR_MSG_TEMPLATE.formatted("read-resource"));
    assertThatThrownBy(() -> read("Observation", OBSERVATION_ID))
        .isExactlyInstanceOf(AccessDeniedError.class)
        .hasMessage(ERROR_MSG_TEMPLATE.formatted("read-resource"));
  }

  @Test
  @DisplayName("US2: the root authority subsumes both the operation and the data authority")
  @WithMockJwt(
      username = "user",
      authorities = {"pathling"})
  void readAllowedWithRootAuthority() {
    assertThat(read("Patient", PATIENT_ID).getIdElement().getIdPart()).isEqualTo(PATIENT_ID);
  }

  // -------------------------------------------------------------------------
  // Helper methods.
  // -------------------------------------------------------------------------

  /**
   * Reads a resource through a provider obtained from the factory, so that both the operation
   * authority check and the inline data authority check apply.
   *
   * @param resourceType the type code of the resource to read
   * @param id the logical identifier of the resource to read
   * @return the resource
   */
  @Nonnull
  private IBaseResource read(@Nonnull final String resourceType, @Nonnull final String id) {
    final ReadProvider provider = readProviderFactory.createReadProvider(resourceType);
    return provider.read(new IdType(resourceType + "/" + id));
  }
}
