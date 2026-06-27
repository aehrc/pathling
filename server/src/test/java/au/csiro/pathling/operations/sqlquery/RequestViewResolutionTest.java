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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.config.AuthorizationConfiguration;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.encoders.ViewDefinitionResource;
import au.csiro.pathling.encoders.ViewDefinitionResource.ColumnComponent;
import au.csiro.pathling.encoders.ViewDefinitionResource.SelectComponent;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.operations.view.ViewExecutionHelper;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import jakarta.annotation.Nonnull;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.hl7.fhir.r4.model.StringType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Tests for the resolution of request-supplied views against canonical dependency references: a
 * supplied view is matched to a reference by its {@code url} and preferred over storage (resolved
 * by {@link ViewResolver}), while a supplied view that carries no {@code url} is rejected at parse
 * time (by {@link SqlQueryExportRequestParser}).
 *
 * @author John Grimes
 */
class RequestViewResolutionTest {

  private static final String PATIENTS_URL = "https://example.org/Patients";

  // ---------------------------------------------------------------------------
  // Supplied-view matching by url (ViewResolver).
  // ---------------------------------------------------------------------------

  @Nested
  class SuppliedViewMatching {

    private ViewResolver resolver;

    @BeforeEach
    void setUp() {
      final ServerConfiguration serverConfiguration = new ServerConfiguration();
      final AuthorizationConfiguration auth = new AuthorizationConfiguration();
      auth.setEnabled(false);
      serverConfiguration.setAuth(auth);
      resolver =
          new ViewResolver(
              mock(DataSource.class),
              mock(FhirEncoders.class),
              serverConfiguration,
              FhirContext.forR4Cached());
    }

    @Test
    void suppliedViewIsMatchedByUrlAndPreferredOverStorage() {
      final var supplied =
          au.csiro.pathling.views.FhirView.ofResource("Patient")
              .select(
                  au.csiro.pathling.views.FhirView.columns(
                      au.csiro.pathling.views.FhirView.column("id", "id")))
              .build();

      final Optional<ResolvedViewDefinition> resolved =
          resolver.resolveSuppliedView(
              new ViewArtifactReference("patients", PATIENTS_URL), Map.of(PATIENTS_URL, supplied));

      assertThat(resolved).isPresent();
      assertThat(resolved.get().getView()).isSameAs(supplied);
      assertThat(resolved.get().getCanonicalKey()).isEqualTo(PATIENTS_URL);
    }

    @Test
    void resolvesNoSuppliedViewWhenNoneMatchesTheUrl() {
      final Optional<ResolvedViewDefinition> resolved =
          resolver.resolveSuppliedView(
              new ViewArtifactReference("patients", PATIENTS_URL), Map.of());

      assertThat(resolved).isEmpty();
    }
  }

  // ---------------------------------------------------------------------------
  // Url-less supplied-view rejection (SqlQueryExportRequestParser).
  // ---------------------------------------------------------------------------

  @Nested
  class SuppliedViewValidation {

    private ViewExecutionHelper viewExecutionHelper;
    private SqlQueryExportRequestParser parser;

    @BeforeEach
    void setUp() {
      viewExecutionHelper = mock(ViewExecutionHelper.class);
      final ServerConfiguration serverConfiguration = new ServerConfiguration();
      final AuthorizationConfiguration auth = new AuthorizationConfiguration();
      auth.setEnabled(false);
      serverConfiguration.setAuth(auth);
      parser =
          new SqlQueryExportRequestParser(
              mock(SqlQueryPipeline.class),
              mock(LibraryReferenceResolver.class),
              viewExecutionHelper,
              FhirContext.forR4Cached(),
              serverConfiguration,
              mock(QueryableDataSource.class));
    }

    @Test
    void rejectsASuppliedViewWithoutAUrl() {
      // The resolved view has no url, so it can never satisfy a canonical reference.
      when(viewExecutionHelper.resolveViewInput(any(), any()))
          .thenReturn(viewDefinitionWithoutUrl());
      final Parameters body = parametersWithInlineView();

      assertThatThrownBy(
              () ->
                  parser.parse(requestDetails(body), null, null, null, null, Set.of(), null, null))
          .isInstanceOf(InvalidRequestException.class)
          .hasMessageContaining("url");
    }

    @Nonnull
    private Parameters parametersWithInlineView() {
      final Parameters parameters = new Parameters();
      final ParametersParameterComponent view = parameters.addParameter().setName("view");
      view.addPart().setName("viewResource").setResource(viewDefinitionWithoutUrl());
      return parameters;
    }

    @Nonnull
    private ServletRequestDetails requestDetails(@Nonnull final Parameters body) {
      final ServletRequestDetails requestDetails = mock(ServletRequestDetails.class);
      when(requestDetails.getResource()).thenReturn(body);
      when(requestDetails.getCompleteUrl()).thenReturn("http://localhost/fhir/$sqlquery-export");
      when(requestDetails.getFhirServerBase()).thenReturn("http://localhost/fhir");
      return requestDetails;
    }
  }

  // ---------------------------------------------------------------------------
  // Helpers.
  // ---------------------------------------------------------------------------

  @Nonnull
  private static ViewDefinitionResource viewDefinitionWithoutUrl() {
    final ViewDefinitionResource view = new ViewDefinitionResource();
    view.setId("no-url-view");
    view.setName(new StringType("no_url_view"));
    view.setResource(new CodeType("Patient"));
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
