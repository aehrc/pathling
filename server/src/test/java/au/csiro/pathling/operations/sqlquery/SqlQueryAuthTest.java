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

package au.csiro.pathling.operations.sqlquery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.config.AuthorizationConfiguration;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.config.SqlQueryConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.encoders.ViewDefinitionResource;
import au.csiro.pathling.encoders.ViewDefinitionResource.ColumnComponent;
import au.csiro.pathling.encoders.ViewDefinitionResource.SelectComponent;
import au.csiro.pathling.errors.AccessDeniedError;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.read.ReadExecutor;
import au.csiro.pathling.views.FhirView;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.Library;
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
 * Security tests for the {@code $sqlquery-*} resolution path, wiring the real {@link ViewResolver},
 * {@link LibraryReferenceResolver}, and {@link SqlDependencyResolver} with authorisation enabled.
 * Verifies the metadata-resource authorisation matrix: a stored ViewDefinition dependency requires
 * {@code ViewDefinition} READ, a stored SQLView dependency requires {@code Library} READ, the
 * per-projected-resource READ still applies at each leaf, and a request-supplied (inline) view
 * requires no metadata READ.
 *
 * @author John Grimes
 */
@Tag("UnitTest")
class SqlQueryAuthTest {

  private ReadExecutor readExecutor;
  private LibraryReferenceResolver libraryReferenceResolver;
  private SqlDependencyResolver resolver;

  @BeforeEach
  void setUp() {
    readExecutor = mock(ReadExecutor.class);
    final ServerConfiguration serverConfiguration = new ServerConfiguration();
    final AuthorizationConfiguration auth = new AuthorizationConfiguration();
    auth.setEnabled(true);
    serverConfiguration.setAuth(auth);
    serverConfiguration.setSqlQuery(new SqlQueryConfiguration());

    final ViewResolver viewResolver =
        new ViewResolver(readExecutor, serverConfiguration, FhirContext.forR4Cached());
    libraryReferenceResolver =
        new LibraryReferenceResolver(
            readExecutor, mock(DataSource.class), mock(FhirEncoders.class), serverConfiguration);
    resolver =
        new SqlDependencyResolver(
            viewResolver, libraryReferenceResolver, new SqlLibraryParser(), serverConfiguration);
  }

  @AfterEach
  void tearDown() {
    SecurityContextHolder.clearContext();
  }

  @Test
  void storedViewDefinitionDependencyRequiresViewDefinitionRead() {
    when(readExecutor.read("ViewDefinition", "pv"))
        .thenReturn(simpleViewDefinition("pv", "Patient"));

    // Projected-resource READ alone is not enough; the ViewDefinition metadata READ is required.
    setSecurityContext("pathling:read:Patient");
    assertThatThrownBy(() -> resolver.resolve(sqlQuery("ViewDefinition/pv"), Map.of()))
        .isInstanceOf(AccessDeniedError.class)
        .hasMessageContaining("ViewDefinition");

    setSecurityContext("pathling:read:ViewDefinition", "pathling:read:Patient");
    assertThatNoException()
        .isThrownBy(() -> resolver.resolve(sqlQuery("ViewDefinition/pv"), Map.of()));
  }

  @Test
  void projectedResourceReadStillRequiredAtTheLeaf() {
    when(readExecutor.read("ViewDefinition", "pv"))
        .thenReturn(simpleViewDefinition("pv", "Patient"));

    // ViewDefinition READ without the projected Patient READ is still denied.
    setSecurityContext("pathling:read:ViewDefinition");
    assertThatThrownBy(() -> resolver.resolve(sqlQuery("ViewDefinition/pv"), Map.of()))
        .isInstanceOf(AccessDeniedError.class)
        .hasMessageContaining("Patient");
  }

  @Test
  void storedSqlViewDependencyRequiresLibraryRead() {
    final Library base = SqlLibraryFixtures.sqlView("SELECT * FROM pv", "pv", "ViewDefinition/pv");
    base.setId("base");
    when(readExecutor.read("Library", "base")).thenReturn(base);
    when(readExecutor.read("ViewDefinition", "pv"))
        .thenReturn(simpleViewDefinition("pv", "Patient"));

    // Holding the transitive ViewDefinition and projected reads, but not Library READ, is denied.
    setSecurityContext("pathling:read:ViewDefinition", "pathling:read:Patient");
    assertThatThrownBy(() -> resolver.resolve(sqlQuery("Library/base"), Map.of()))
        .isInstanceOf(AccessDeniedError.class)
        .hasMessageContaining("Library");

    setSecurityContext(
        "pathling:read:Library", "pathling:read:ViewDefinition", "pathling:read:Patient");
    assertThatNoException().isThrownBy(() -> resolver.resolve(sqlQuery("Library/base"), Map.of()));
  }

  @Test
  void inlineSuppliedViewRequiresNoMetadataRead() {
    // A request-supplied (inline) view is not read from storage, so resolving it needs no metadata
    // READ - even with no authorities granted.
    setSecurityContext("pathling:sqlquery-run");
    final FhirView supplied =
        FhirView.ofResource("Patient")
            .select(FhirView.columns(FhirView.column("id", "id")))
            .build();

    assertThatNoException()
        .isThrownBy(() -> resolver.resolve(sqlQuery("ViewDefinition/pv"), Map.of("pv", supplied)));
  }

  @Test
  void topLevelQueryReferenceRequiresLibraryRead() {
    final Library base = SqlLibraryFixtures.sqlView("SELECT 1");
    base.setId("base");
    when(readExecutor.read("Library", "base")).thenReturn(base);

    // The top-level by-reference resolution goes through LibraryReferenceResolver, which enforces
    // the Library metadata READ.
    setSecurityContext("pathling:sqlquery-run");
    assertThatThrownBy(() -> libraryReferenceResolver.resolve(new Reference("Library/base")))
        .isInstanceOf(AccessDeniedError.class)
        .hasMessageContaining("Library");

    setSecurityContext("pathling:read:Library");
    assertThat(libraryReferenceResolver.resolve(new Reference("Library/base"))).isNotNull();
  }

  @Nonnull
  private static ParsedSqlQuery sqlQuery(@Nonnull final String resource) {
    return new ParsedSqlQuery(
        "SELECT * FROM t",
        List.of(new ViewArtifactReference("t", resource)),
        List.of(),
        SqlLibraryParser.SQL_QUERY_TYPE_CODE);
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
