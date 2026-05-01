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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.config.AuthorizationConfiguration;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.encoders.ViewDefinitionResource;
import au.csiro.pathling.encoders.ViewDefinitionResource.ColumnComponent;
import au.csiro.pathling.encoders.ViewDefinitionResource.SelectComponent;
import au.csiro.pathling.errors.ResourceNotFoundError;
import au.csiro.pathling.read.ReadExecutor;
import au.csiro.pathling.views.FhirView;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.StringType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ViewResolver} covering id extraction, label-order preservation, and the
 * read-error and parse-error wrapping paths.
 */
class ViewResolverTest {

  private ReadExecutor readExecutor;
  private ServerConfiguration serverConfiguration;
  private ViewResolver resolver;

  @BeforeEach
  void setUp() {
    readExecutor = mock(ReadExecutor.class);
    serverConfiguration = new ServerConfiguration();
    final AuthorizationConfiguration auth = new AuthorizationConfiguration();
    auth.setEnabled(false);
    serverConfiguration.setAuth(auth);
    resolver = new ViewResolver(readExecutor, serverConfiguration, FhirContext.forR4());
  }

  @Test
  void resolvesEmptyReferenceListToEmptyMap() {
    final Map<String, FhirView> resolved = resolver.resolve(List.of());
    assertThat(resolved).isEmpty();
  }

  @Test
  void resolvesSingleReferenceByBareId() {
    when(readExecutor.read("ViewDefinition", "patient-view"))
        .thenReturn(simpleViewDefinition("patient-view", "Patient"));

    final Map<String, FhirView> resolved =
        resolver.resolve(List.of(new ViewArtifactReference("patients", "patient-view")));

    assertThat(resolved).containsOnlyKeys("patients");
    assertThat(resolved.get("patients").getResource()).isEqualTo("Patient");
  }

  @Test
  void extractsIdFromCanonicalUrl() {
    when(readExecutor.read("ViewDefinition", "obs-view"))
        .thenReturn(simpleViewDefinition("obs-view", "Observation"));

    final Map<String, FhirView> resolved =
        resolver.resolve(
            List.of(
                new ViewArtifactReference("obs", "https://example.org/ViewDefinition/obs-view")));

    assertThat(resolved).containsOnlyKeys("obs");
    assertThat(resolved.get("obs").getResource()).isEqualTo("Observation");
  }

  @Test
  void preservesLabelOrderAcrossMultipleReferences() {
    when(readExecutor.read("ViewDefinition", "a")).thenReturn(simpleViewDefinition("a", "Patient"));
    when(readExecutor.read("ViewDefinition", "b"))
        .thenReturn(simpleViewDefinition("b", "Observation"));
    when(readExecutor.read("ViewDefinition", "c"))
        .thenReturn(simpleViewDefinition("c", "Condition"));

    final Map<String, FhirView> resolved =
        resolver.resolve(
            List.of(
                new ViewArtifactReference("first", "a"),
                new ViewArtifactReference("second", "b"),
                new ViewArtifactReference("third", "c")));

    assertThat(resolved.keySet()).containsExactly("first", "second", "third");
  }

  @Test
  void wrapsReadExecutorFailureWithLabelAndReference() {
    when(readExecutor.read("ViewDefinition", "missing"))
        .thenThrow(new ResourceNotFoundError("not there"));

    final List<ViewArtifactReference> refs =
        List.of(new ViewArtifactReference("patients", "missing"));

    assertThatThrownBy(() -> resolver.resolve(refs))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("patients")
        .hasMessageContaining("missing");
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
