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

package au.csiro.pathling.operations.view;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.config.AuthorizationConfiguration;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.encoders.ViewDefinitionResource;
import au.csiro.pathling.encoders.ViewDefinitionResource.ColumnComponent;
import au.csiro.pathling.encoders.ViewDefinitionResource.SelectComponent;
import au.csiro.pathling.errors.AccessDeniedError;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.operations.compartment.GroupMemberService;
import au.csiro.pathling.operations.compartment.PatientCompartmentService;
import au.csiro.pathling.read.ReadExecutor;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.Reference;
import org.hl7.fhir.r4.model.StringType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.security.core.authority.AuthorityUtils;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;

/**
 * Security tests for {@link ViewExecutionHelper}, verifying that resolving a stored {@code
 * ViewDefinition} from a {@code viewReference} requires the {@code ViewDefinition} metadata READ
 * authority, while an inline {@code viewResource} requires no such check.
 *
 * @author John Grimes
 */
@Tag("UnitTest")
class ViewExecutionHelperAuthTest {

  private ReadExecutor readExecutor;
  private AuthorizationConfiguration authConfig;
  private ViewExecutionHelper helper;

  @BeforeEach
  void setUp() {
    readExecutor = mock(ReadExecutor.class);
    final ServerConfiguration serverConfiguration = new ServerConfiguration();
    authConfig = new AuthorizationConfiguration();
    authConfig.setEnabled(true);
    serverConfiguration.setAuth(authConfig);
    helper =
        new ViewExecutionHelper(
            mock(SparkSession.class),
            mock(QueryableDataSource.class),
            FhirContext.forR4Cached(),
            mock(FhirEncoders.class),
            mock(PatientCompartmentService.class),
            mock(GroupMemberService.class),
            serverConfiguration,
            readExecutor);
  }

  @AfterEach
  void tearDown() {
    SecurityContextHolder.clearContext();
  }

  @Test
  void viewReferenceRequiresViewDefinitionReadAuthority() {
    setSecurityContext("pathling:read:Patient");
    when(readExecutor.read("ViewDefinition", "stored"))
        .thenReturn(simpleViewDefinition("stored", "Patient"));

    assertThatThrownBy(() -> helper.resolveViewInput(null, new Reference("ViewDefinition/stored")))
        .isInstanceOf(AccessDeniedError.class)
        .hasMessageContaining("ViewDefinition");
  }

  @Test
  void viewReferenceSucceedsWithViewDefinitionReadAuthority() {
    setSecurityContext("pathling:read:ViewDefinition");
    when(readExecutor.read("ViewDefinition", "stored"))
        .thenReturn(simpleViewDefinition("stored", "Patient"));

    assertThat(helper.resolveViewInput(null, new Reference("ViewDefinition/stored"))).isNotNull();
  }

  @Test
  void inlineViewResourceRequiresNoViewDefinitionRead() {
    // An inline viewResource is not read from storage, so the metadata READ check does not apply.
    setSecurityContext("pathling:search");
    final ViewDefinitionResource inline = simpleViewDefinition("inline", "Patient");

    assertThat(helper.resolveViewInput(inline, null)).isSameAs(inline);
  }

  private void setSecurityContext(final String... authorities) {
    final Jwt jwt = Jwt.withTokenValue("mock").header("alg", "none").claim("sub", "user").build();
    final JwtAuthenticationToken auth =
        new JwtAuthenticationToken(jwt, AuthorityUtils.createAuthorityList(authorities));
    SecurityContextHolder.getContext().setAuthentication(auth);
  }

  @Nonnull
  private static ViewDefinitionResource simpleViewDefinition(
      @Nonnull final String id, @Nonnull final String resourceType) {
    final ViewDefinitionResource view = new ViewDefinitionResource();
    view.setId(id);
    view.setName(new StringType(id + "_view"));
    view.setResource(new CodeType(resourceType));
    view.setStatus(new CodeType("active"));
    final SelectComponent select = new SelectComponent();
    final ColumnComponent column = new ColumnComponent();
    column.setName(new StringType("id"));
    column.setPath(new StringType("id"));
    select.getColumn().add(column);
    view.getSelect().add(select);
    return view;
  }
}
