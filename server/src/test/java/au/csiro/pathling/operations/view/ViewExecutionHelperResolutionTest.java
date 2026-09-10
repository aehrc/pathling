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

import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.encoders.ViewDefinitionResource;
import au.csiro.pathling.encoders.ViewDefinitionResource.ColumnComponent;
import au.csiro.pathling.encoders.ViewDefinitionResource.SelectComponent;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.operations.compartment.GroupMemberService;
import au.csiro.pathling.operations.compartment.PatientCompartmentService;
import au.csiro.pathling.read.ReadExecutor;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.util.CustomObjectDataSource;
import au.csiro.pathling.util.FhirServerTestConfiguration;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import jakarta.annotation.Nonnull;
import java.util.List;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.Reference;
import org.hl7.fhir.r4.model.StringType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;

/**
 * Tests for {@link ViewExecutionHelper#resolveViewInput} and {@link
 * ViewExecutionHelper#readStoredViewDefinition}, exercising {@code viewReference} resolution
 * against a stored ViewDefinition (US4, FR-009..FR-013). Uses a {@link CustomObjectDataSource}
 * backing the read path, because newly created ViewDefinitions are not visible to the
 * integration-test server's cached Delta Lake source.
 *
 * @author John Grimes
 */
@Import({
  FhirServerTestConfiguration.class,
  PatientCompartmentService.class,
  GroupMemberService.class
})
@SpringBootUnitTest
class ViewExecutionHelperResolutionTest {

  @Autowired private SparkSession sparkSession;
  @Autowired private PathlingContext pathlingContext;
  @Autowired private FhirEncoders fhirEncoders;
  @Autowired private FhirContext fhirContext;
  @Autowired private QueryableDataSource deltaLake;
  @Autowired private PatientCompartmentService patientCompartmentService;
  @Autowired private GroupMemberService groupMemberService;
  @Autowired private ServerConfiguration serverConfiguration;

  private ViewExecutionHelper helper;

  @BeforeEach
  void setUp() {
    // Back the read path with a CustomObjectDataSource that holds a stored ViewDefinition.
    final List<IBaseResource> stored =
        List.of(createSimpleViewDefinition("stored-view", "stored_view", "Patient"));
    final CustomObjectDataSource dataSource =
        new CustomObjectDataSource(sparkSession, pathlingContext, fhirEncoders, stored);
    final ReadExecutor readExecutor = new ReadExecutor(dataSource, fhirEncoders);

    helper =
        new ViewExecutionHelper(
            sparkSession,
            deltaLake,
            fhirContext,
            fhirEncoders,
            patientCompartmentService,
            groupMemberService,
            serverConfiguration,
            readExecutor);
  }

  @Test
  void resolvesViewReferenceToStoredViewDefinition() {
    // A viewReference of the form ViewDefinition/{id} resolves to the stored ViewDefinition.
    final IBaseResource resolved =
        helper.resolveViewInput(null, new Reference("ViewDefinition/stored-view"));

    assertThat(resolved).isInstanceOf(ViewDefinitionResource.class);
    assertThat(((ViewDefinitionResource) resolved).getIdElement().getIdPart())
        .isEqualTo("stored-view");
  }

  @Test
  void returnsInlineViewResourceWhenSupplied() {
    final ViewDefinitionResource inline =
        createSimpleViewDefinition("inline", "inline_view", "Patient");
    assertThat(helper.resolveViewInput(inline, null)).isSameAs(inline);
  }

  @Test
  void rejectsUnresolvedViewReferenceWithNotFound() {
    assertThatThrownBy(
            () -> helper.resolveViewInput(null, new Reference("ViewDefinition/does-not-exist")))
        .isInstanceOf(ResourceNotFoundException.class);
  }

  @Test
  void rejectsBothViewResourceAndViewReference() {
    final ViewDefinitionResource inline =
        createSimpleViewDefinition("inline", "inline_view", "Patient");
    assertThatThrownBy(
            () -> helper.resolveViewInput(inline, new Reference("ViewDefinition/stored-view")))
        .isInstanceOf(InvalidRequestException.class);
  }

  @Test
  void rejectsNeitherViewResourceNorViewReference() {
    assertThatThrownBy(() -> helper.resolveViewInput(null, null))
        .isInstanceOf(InvalidRequestException.class);
  }

  /** Builds a minimal stored ViewDefinition over the given resource type. */
  @Nonnull
  private ViewDefinitionResource createSimpleViewDefinition(
      @Nonnull final String id, @Nonnull final String name, @Nonnull final String resource) {
    final ViewDefinitionResource view = new ViewDefinitionResource();
    view.setId(id);
    view.setName(new StringType(name));
    view.setResource(new CodeType(resource));
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
