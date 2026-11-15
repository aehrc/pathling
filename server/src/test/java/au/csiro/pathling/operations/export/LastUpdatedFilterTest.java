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

package au.csiro.pathling.operations.export;

import static org.assertj.core.api.Assertions.assertThat;

import au.csiro.pathling.io.ResourceEncoding;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.util.FhirServerTestConfiguration;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.InstantType;
import org.hl7.fhir.r4.model.Patient;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;

/**
 * Tests for {@link LastUpdatedFilter}, which filters stored resources on {@code meta.lastUpdated}
 * stored as text that may be absent from the schema altogether.
 *
 * @author John Grimes
 */
@Import(FhirServerTestConfiguration.class)
@SpringBootUnitTest
class LastUpdatedFilterTest {

  @Autowired private PathlingContext pathlingContext;

  // Values stated in different time zones are compared as instants, not as strings: 09:00 at +10:00
  // is 23:00 UTC on the previous day, which a string comparison would place after 2024-01-01.
  @Test
  void sinceComparesInstantsAcrossTimeZones() {
    final Dataset<Row> resources =
        encode(
            patient("before", "2024-01-01T09:00:00+10:00"),
            patient("after", "2024-01-01T00:30:00Z"),
            patient("unknown", null));

    final Dataset<Row> result =
        LastUpdatedFilter.since(resources, new InstantType("2024-01-01T00:00:00Z"));

    assertThat(ids(result)).containsExactlyInAnyOrder("after", "unknown");
  }

  @Test
  void untilComparesInstantsAcrossTimeZones() {
    final Dataset<Row> resources =
        encode(
            patient("before", "2024-01-01T09:00:00+10:00"),
            patient("after", "2024-01-01T00:30:00Z"),
            patient("unknown", null));

    final Dataset<Row> result =
        LastUpdatedFilter.until(resources, new InstantType("2024-01-01T00:00:00Z"));

    assertThat(ids(result)).containsExactlyInAnyOrder("before", "unknown");
  }

  // Where no resource carries meta.lastUpdated, the dataset has no such column, and every resource
  // is kept because none has a known time of last update.
  @Test
  void keepsEverythingWhenLastUpdatedIsAbsentFromSchema() {
    final Patient withProfileOnly = patient("profiled", null);
    withProfileOnly.getMeta().addProfile("http://example.org/StructureDefinition/p");
    final Dataset<Row> resources = encode(withProfileOnly, patient("bare", null));

    final Dataset<Row> since =
        LastUpdatedFilter.since(resources, new InstantType("2024-01-01T00:00:00Z"));
    final Dataset<Row> until =
        LastUpdatedFilter.until(resources, new InstantType("2024-01-01T00:00:00Z"));

    assertThat(ids(since)).containsExactlyInAnyOrder("profiled", "bare");
    assertThat(ids(until)).containsExactlyInAnyOrder("profiled", "bare");
  }

  @Nonnull
  private Dataset<Row> encode(@Nonnull final Patient... patients) {
    return ResourceEncoding.encode(pathlingContext, "Patient", List.of(patients));
  }

  @Nonnull
  private static List<String> ids(@Nonnull final Dataset<Row> resources) {
    return resources.select("id").as(Encoders.STRING()).collectAsList();
  }

  @Nonnull
  private static Patient patient(@Nonnull final String id, @Nullable final String lastUpdated) {
    final Patient patient = new Patient();
    patient.setId(id);
    if (lastUpdated != null) {
      patient.getMeta().setLastUpdatedElement(new InstantType(lastUpdated));
    }
    return patient;
  }
}
