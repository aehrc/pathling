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

package au.csiro.pathling.operations.update;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

import au.csiro.pathling.config.AuthorizationConfiguration;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.errors.AccessDeniedError;
import au.csiro.pathling.operations.delete.DeleteExecutor;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.r4.model.Bundle.BundleEntryRequestComponent;
import org.hl7.fhir.r4.model.Bundle.BundleType;
import org.hl7.fhir.r4.model.Bundle.HTTPVerb;
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
 * Security tests for {@link BatchProvider} verifying per-resource write authority enforcement for
 * create, update, and delete entries.
 *
 * @author John Grimes
 */
@Tag("UnitTest")
@ExtendWith(MockitoExtension.class)
class BatchProviderAuthTest {

  @Mock private ServerConfiguration configuration;
  @Mock private UpdateExecutor updateExecutor;
  @Mock private DeleteExecutor deleteExecutor;

  private BatchProvider batchProvider;
  private AuthorizationConfiguration authConfig;

  @BeforeEach
  void setUp() {
    authConfig = new AuthorizationConfiguration();
    when(configuration.getAuth()).thenReturn(authConfig);
    batchProvider = new BatchProvider(updateExecutor, deleteExecutor, configuration);
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
  void batchCreateSucceedsWhenAuthDisabled() {
    authConfig.setEnabled(false);
    final Bundle bundle = createBundleWithCreateEntry();
    assertThat(batchProvider.batch(bundle)).isNotNull();
  }

  @Test
  void batchCreateSucceedsWithCorrectResourceAuthority() {
    authConfig.setEnabled(true);
    setSecurityContext("pathling:batch", "pathling:write:Patient", "pathling:update");
    final Bundle bundle = createBundleWithCreateEntry();
    assertThat(batchProvider.batch(bundle)).isNotNull();
  }

  @Test
  void batchCreateThrowsWhenMissingResourceAuthority() {
    authConfig.setEnabled(true);
    setSecurityContext("pathling:batch", "pathling:update");
    final Bundle bundle = createBundleWithCreateEntry();
    assertThatThrownBy(() -> batchProvider.batch(bundle))
        .isInstanceOf(AccessDeniedError.class)
        .hasMessageContaining("pathling:write:Patient");
  }

  @Test
  void batchUpdateSucceedsWhenAuthDisabled() {
    authConfig.setEnabled(false);
    final Bundle bundle = createBundleWithUpdateEntry();
    assertThat(batchProvider.batch(bundle)).isNotNull();
  }

  @Test
  void batchUpdateSucceedsWithCorrectResourceAuthority() {
    authConfig.setEnabled(true);
    setSecurityContext("pathling:batch", "pathling:write:Patient", "pathling:update");
    final Bundle bundle = createBundleWithUpdateEntry();
    assertThat(batchProvider.batch(bundle)).isNotNull();
  }

  @Test
  void batchUpdateThrowsWhenMissingResourceAuthority() {
    authConfig.setEnabled(true);
    setSecurityContext("pathling:batch", "pathling:update");
    final Bundle bundle = createBundleWithUpdateEntry();
    assertThatThrownBy(() -> batchProvider.batch(bundle))
        .isInstanceOf(AccessDeniedError.class)
        .hasMessageContaining("pathling:write:Patient");
  }

  @Test
  void batchDeleteSucceedsWhenAuthDisabled() {
    authConfig.setEnabled(false);
    final Bundle bundle = createBundleWithDeleteEntry();
    assertThat(batchProvider.batch(bundle)).isNotNull();
  }

  @Test
  void batchDeleteSucceedsWithCorrectResourceAuthority() {
    authConfig.setEnabled(true);
    setSecurityContext("pathling:batch", "pathling:write:Patient", "pathling:delete");
    final Bundle bundle = createBundleWithDeleteEntry();
    assertThat(batchProvider.batch(bundle)).isNotNull();
  }

  @Test
  void batchDeleteThrowsWhenMissingResourceAuthority() {
    authConfig.setEnabled(true);
    setSecurityContext("pathling:batch", "pathling:delete");
    final Bundle bundle = createBundleWithDeleteEntry();
    assertThatThrownBy(() -> batchProvider.batch(bundle))
        .isInstanceOf(AccessDeniedError.class)
        .hasMessageContaining("pathling:write:Patient");
  }

  private Bundle createBundleWithCreateEntry() {
    final Bundle bundle = new Bundle();
    bundle.setType(BundleType.BATCH);

    final Patient patient = new Patient();
    final BundleEntryComponent entry = bundle.addEntry();
    entry.setResource(patient);
    final BundleEntryRequestComponent request = new BundleEntryRequestComponent();
    request.setMethod(HTTPVerb.POST);
    request.setUrl("Patient");
    entry.setRequest(request);

    return bundle;
  }

  private Bundle createBundleWithUpdateEntry() {
    final Bundle bundle = new Bundle();
    bundle.setType(BundleType.BATCH);

    final Patient patient = new Patient();
    patient.setId("patient-1");
    final BundleEntryComponent entry = bundle.addEntry();
    entry.setResource(patient);
    final BundleEntryRequestComponent request = new BundleEntryRequestComponent();
    request.setMethod(HTTPVerb.PUT);
    request.setUrl("Patient/patient-1");
    entry.setRequest(request);

    return bundle;
  }

  private Bundle createBundleWithDeleteEntry() {
    final Bundle bundle = new Bundle();
    bundle.setType(BundleType.BATCH);

    final BundleEntryComponent entry = bundle.addEntry();
    final BundleEntryRequestComponent request = new BundleEntryRequestComponent();
    request.setMethod(HTTPVerb.DELETE);
    request.setUrl("Patient/patient-1");
    entry.setRequest(request);

    return bundle;
  }
}
