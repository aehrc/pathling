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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import au.csiro.pathling.config.AuthorizationConfiguration;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.encoders.ViewDefinitionResource;
import au.csiro.pathling.encoders.ViewDefinitionResource.ColumnComponent;
import au.csiro.pathling.encoders.ViewDefinitionResource.SelectComponent;
import au.csiro.pathling.read.ReadExecutor;
import au.csiro.pathling.views.FhirView;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.StringType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the request-supplied view resolution path of {@link ViewResolver}: a supplied view
 * is matched to a {@code relatedArtifact} reference by id and preferred over server storage, while
 * a reference with no matching supplied view falls back to storage.
 *
 * @author John Grimes
 */
class RequestViewResolutionTest {

  private ReadExecutor readExecutor;
  private ViewResolver resolver;

  @BeforeEach
  void setUp() {
    readExecutor = mock(ReadExecutor.class);
    final ServerConfiguration serverConfiguration = new ServerConfiguration();
    final AuthorizationConfiguration auth = new AuthorizationConfiguration();
    auth.setEnabled(false);
    serverConfiguration.setAuth(auth);
    resolver = new ViewResolver(readExecutor, serverConfiguration, FhirContext.forR4());
  }

  @Test
  void suppliedViewIsPreferredAndStorageIsNotConsulted() {
    final FhirView supplied =
        FhirView.ofResource("Patient")
            .select(FhirView.columns(FhirView.column("id", "id")))
            .build();
    final Map<String, FhirView> suppliedViews = Map.of("patient-bp", supplied);

    final Map<String, FhirView> resolved =
        resolver.resolve(
            List.of(new ViewArtifactReference("patients", "ViewDefinition/patient-bp")),
            suppliedViews);

    assertThat(resolved.get("patients")).isSameAs(supplied);
    verify(readExecutor, never())
        .read(
            org.mockito.ArgumentMatchers.eq("ViewDefinition"),
            org.mockito.ArgumentMatchers.anyString());
  }

  @Test
  void referenceWithNoSuppliedViewFallsBackToStorage() {
    when(readExecutor.read("ViewDefinition", "patient-bp"))
        .thenReturn(simpleViewDefinition("patient-bp", "Patient"));

    final Map<String, FhirView> resolved =
        resolver.resolve(
            List.of(new ViewArtifactReference("patients", "ViewDefinition/patient-bp")), Map.of());

    assertThat(resolved).containsOnlyKeys("patients");
    verify(readExecutor).read("ViewDefinition", "patient-bp");
  }

  @Test
  void mixesSuppliedAndStorageResolvedViews() {
    final FhirView supplied =
        FhirView.ofResource("Patient")
            .select(FhirView.columns(FhirView.column("id", "id")))
            .build();
    when(readExecutor.read("ViewDefinition", "obs-view"))
        .thenReturn(simpleViewDefinition("obs-view", "Observation"));

    final Map<String, FhirView> resolved =
        resolver.resolve(
            List.of(
                new ViewArtifactReference("patients", "ViewDefinition/patient-bp"),
                new ViewArtifactReference("obs", "ViewDefinition/obs-view")),
            Map.of("patient-bp", supplied));

    assertThat(resolved.get("patients")).isSameAs(supplied);
    assertThat(resolved.get("obs").getResource()).isEqualTo("Observation");
    verify(readExecutor).read("ViewDefinition", "obs-view");
    verify(readExecutor, never()).read("ViewDefinition", "patient-bp");
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
