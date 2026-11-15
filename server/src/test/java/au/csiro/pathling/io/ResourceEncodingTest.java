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

package au.csiro.pathling.io;

import static org.assertj.core.api.Assertions.assertThat;

import au.csiro.pathling.encoders.ViewDefinitionResource;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.LayoutDetector;
import au.csiro.pathling.library.io.source.StorageLayout;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.util.FhirServerTestConfiguration;
import java.math.BigDecimal;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations.AdministrativeGender;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Quantity;
import org.hl7.fhir.r4.model.StringType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;

/**
 * Unit tests for {@link ResourceEncoding}, which moves resources between HAPI objects and datasets
 * in the Parquet on FHIR layout.
 *
 * @author John Grimes
 */
@Import(FhirServerTestConfiguration.class)
@SpringBootUnitTest
class ResourceEncodingTest {

  @Autowired private PathlingContext pathlingContext;

  // Resources are encoded in the Parquet on FHIR layout, one row per resource.
  @Test
  void encodesResourcesInTheParquetOnFhirLayout() {
    final Dataset<Row> rows =
        ResourceEncoding.encode(pathlingContext, "Patient", List.of(patient("p1"), patient("p2")));

    assertThat(LayoutDetector.detect(rows.schema()).getLayout())
        .isEqualTo(StorageLayout.PARQUET_ON_FHIR);
    assertThat(rows.count()).isEqualTo(2);
  }

  // Encoding then decoding returns the resources, in the order of the rows.
  @Test
  void decodesWhatItEncodes() {
    final Observation observation = new Observation();
    observation.setId("o1");
    observation.setStatus(Observation.ObservationStatus.FINAL);
    observation.setValue(new Quantity().setValue(new BigDecimal("1.25")).setUnit("mg"));

    final List<IBaseResource> patients =
        ResourceEncoding.decode(
            pathlingContext,
            "Patient",
            ResourceEncoding.encode(
                pathlingContext, "Patient", List.of(patient("p1"), patient("p2"))));
    final List<IBaseResource> observations =
        ResourceEncoding.decode(
            pathlingContext,
            "Observation",
            ResourceEncoding.encode(pathlingContext, "Observation", List.of(observation)));

    assertThat(patients)
        .extracting(resource -> resource.getIdElement().getIdPart())
        .containsExactly("p1", "p2");
    assertThat(((Patient) patients.get(0)).getGender()).isEqualTo(AdministrativeGender.MALE);
    assertThat(((Observation) observations.get(0)).getValueQuantity().getValue())
        .isEqualByComparingTo("1.25");
  }

  // The custom ViewDefinition resource type is encoded and decoded like any other.
  @Test
  void roundTripsViewDefinitions() {
    final ViewDefinitionResource view = new ViewDefinitionResource();
    view.setId("v1");
    view.setName(new StringType("patients"));
    view.setResource(new org.hl7.fhir.r4.model.CodeType("Patient"));

    final List<IBaseResource> decoded =
        ResourceEncoding.decode(
            pathlingContext,
            "ViewDefinition",
            ResourceEncoding.encode(pathlingContext, "ViewDefinition", List.of(view)));

    assertThat(decoded).hasSize(1);
    assertThat(decoded.get(0)).isInstanceOf(ViewDefinitionResource.class);
    final ViewDefinitionResource result = (ViewDefinitionResource) decoded.get(0);
    assertThat(result.getIdElement().getIdPart()).isEqualTo("v1");
    assertThat(result.getName().getValue()).isEqualTo("patients");
  }

  // An empty dataset of a type has no rows, is in the Parquet on FHIR layout, and has an id column
  // to filter on.
  @Test
  void createsEmptyDatasets() {
    final Dataset<Row> empty = ResourceEncoding.empty(pathlingContext, "Condition");

    assertThat(empty.count()).isZero();
    assertThat(empty.schema().fieldNames()).contains("id");
    assertThat(LayoutDetector.detect(empty.schema()).isSupported()).isTrue();
    assertThat(ResourceEncoding.decode(pathlingContext, "Condition", empty)).isEmpty();
  }

  private static Patient patient(final String id) {
    final Patient patient = new Patient();
    patient.setId(id);
    patient.setGender(AdministrativeGender.MALE);
    return patient;
  }
}
