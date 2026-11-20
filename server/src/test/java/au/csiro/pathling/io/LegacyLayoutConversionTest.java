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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.LayoutDetector;
import au.csiro.pathling.library.io.source.StorageLayout;
import au.csiro.pathling.library.io.source.UnsupportedLayoutException;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.util.FhirServerTestConfiguration;
import ca.uhn.fhir.parser.IParser;
import jakarta.annotation.Nonnull;
import java.math.BigDecimal;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations.AdministrativeGender;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Quantity;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;

/**
 * Unit tests for {@link LegacyLayoutConversion}, which converts data in the layout written before
 * Parquet on FHIR into the Parquet on FHIR layout.
 *
 * @author John Grimes
 */
@Import(FhirServerTestConfiguration.class)
@SpringBootUnitTest
class LegacyLayoutConversionTest {

  @Autowired private PathlingContext pathlingContext;

  @Autowired private FhirEncoders fhirEncoders;

  @Autowired private IParser jsonParser;

  // A table written by the legacy encoders is recognised as the legacy layout, and a table encoded
  // by the current library is not.
  @Test
  void recognisesTheLegacyLayout() {
    assertThat(LegacyLayoutConversion.isLegacy(legacyDataset("Patient", patient()).schema()))
        .isTrue();
    assertThat(LegacyLayoutConversion.isLegacy(currentDataset("Patient", patient()).schema()))
        .isFalse();
  }

  // Legacy-layout data is converted to the Parquet on FHIR layout, and the resources it holds
  // survive the conversion.
  @Test
  void convertsLegacyDataToParquetOnFhir() {
    final Dataset<Row> converted =
        LegacyLayoutConversion.convertIfLegacy(
            pathlingContext, "Patient", legacyDataset("Patient", patient()));

    assertThat(LayoutDetector.detect(converted.schema()).getLayout())
        .isEqualTo(StorageLayout.PARQUET_ON_FHIR);
    final List<Patient> patients = decode(converted, "Patient", Patient.class);
    assertThat(patients).hasSize(1);
    final Patient result = patients.get(0);
    assertThat(result.getIdPart()).isEqualTo("p1");
    assertThat(result.getGender()).isEqualTo(AdministrativeGender.FEMALE);
    assertThat(result.getNameFirstRep().getFamily()).isEqualTo("Smith");
    assertThat(result.getBirthDateElement().getValueAsString()).isEqualTo("1970-01-02");
  }

  // A decimal survives the conversion with its value intact. The legacy layout stored decimals at
  // a fixed scale, so the scale of the result is not asserted.
  @Test
  void convertsDecimalsWithoutChangingTheirValue() {
    final Observation observation = new Observation();
    observation.setId("o1");
    observation.setStatus(Observation.ObservationStatus.FINAL);
    observation.setValue(new Quantity().setValue(new BigDecimal("1.5")).setUnit("mmol/L"));

    final Dataset<Row> converted =
        LegacyLayoutConversion.convertIfLegacy(
            pathlingContext, "Observation", legacyDataset("Observation", observation));

    final Observation result = decode(converted, "Observation", Observation.class).get(0);
    assertThat(result.getValueQuantity().getValue()).isEqualByComparingTo("1.5");
    assertThat(result.getValueQuantity().getUnit()).isEqualTo("mmol/L");
  }

  // Data already in the Parquet on FHIR layout is returned as it is.
  @Test
  void returnsParquetOnFhirDataUnchanged() {
    final Dataset<Row> current = currentDataset("Patient", patient());

    assertThat(LegacyLayoutConversion.convertIfLegacy(pathlingContext, "Patient", current))
        .isSameAs(current);
  }

  // An empty legacy-layout table converts to an empty table in the Parquet on FHIR layout.
  @Test
  void convertsAnEmptyLegacyTable() {
    final Dataset<Row> converted =
        LegacyLayoutConversion.convertIfLegacy(
            pathlingContext, "Patient", legacyDataset("Patient"));

    assertThat(LegacyLayoutConversion.isLegacy(converted.schema())).isFalse();
    assertThat(converted.count()).isZero();
  }

  // Data that mixes the two layouts cannot be converted, because the legacy decoder cannot read
  // the rows written in the new layout. It is refused with the library's exception.
  @Test
  void refusesDataMixingTheTwoLayouts() {
    final Dataset<Row> mixed =
        legacyDataset("Patient", patient()).withColumn("resourceType", functions.lit("Patient"));

    assertThatThrownBy(
            () -> LegacyLayoutConversion.convertIfLegacy(pathlingContext, "Patient", mixed))
        .isInstanceOf(UnsupportedLayoutException.class);
  }

  @Nonnull
  private static Patient patient() {
    final Patient patient = new Patient();
    patient.setId("p1");
    patient.setGender(AdministrativeGender.FEMALE);
    patient.addName().setFamily("Smith");
    patient.getBirthDateElement().setValueAsString("1970-01-02");
    return patient;
  }

  @Nonnull
  private Dataset<Row> legacyDataset(
      @Nonnull final String resourceType, @Nonnull final IBaseResource... resources) {
    return pathlingContext
        .getSpark()
        .createDataset(List.of(resources), fhirEncoders.of(resourceType))
        .toDF();
  }

  @Nonnull
  private Dataset<Row> currentDataset(
      @Nonnull final String resourceType, @Nonnull final IBaseResource... resources) {
    final List<String> json =
        List.of(resources).stream().map(jsonParser::encodeResourceToString).toList();
    return pathlingContext.encode(
        pathlingContext.getSpark().createDataset(json, Encoders.STRING()).toDF(), resourceType);
  }

  @Nonnull
  private <T extends IBaseResource> List<T> decode(
      @Nonnull final Dataset<Row> rows,
      @Nonnull final String resourceType,
      @Nonnull final Class<T> type) {
    return pathlingContext
        .decode(rows, resourceType, "application/fhir+json")
        .collectAsList()
        .stream()
        .map(json -> jsonParser.parseResource(type, json))
        .toList();
  }
}
