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

import static org.junit.jupiter.api.Assertions.fail;

import au.csiro.pathling.io.egress.ResourceSerialiser;
import au.csiro.pathling.io.transform.TransformFixtures;
import jakarta.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

/**
 * Tests what the output must leave out rather than write as empty (FR-019).
 *
 * <p>A stored row says nothing about how it came to look as it does: an element the source never
 * carried and one whose every part was dropped are the same row. The output has to be the same
 * either way, which is the absence of the key rather than a null, an empty object or an array of
 * nulls. The rows are built directly rather than ingested, because the shapes under test are
 * exactly the ones an ingest is careful not to produce.
 */
class EgressOmissionTest {

  /** A fragment of the stored Patient layout, wide enough to carry a structure and an array. */
  @Nonnull
  private static final StructType SCHEMA =
      new StructType(
          new StructField[] {
            field("resourceType", DataTypes.StringType),
            field("id", DataTypes.StringType),
            field(
                "name",
                DataTypes.createArrayType(
                    new StructType(
                        new StructField[] {
                          field("family", DataTypes.StringType),
                          field("given", DataTypes.createArrayType(DataTypes.StringType, true))
                        }),
                    true)),
            field(
                "maritalStatus",
                new StructType(new StructField[] {field("text", DataTypes.StringType)}))
          });

  @Test
  void omitsAnAbsentElementRatherThanWritingItAsNull() {
    assertSerialises(
        RowFactory.create("Patient", "absent", null, null),
        "{\"resourceType\":\"Patient\",\"id\":\"absent\"}");
  }

  @Test
  void omitsAStructureWhoseEveryFieldIsNull() {
    assertSerialises(
        RowFactory.create("Patient", "empty-structure", null, RowFactory.create((Object) null)),
        "{\"resourceType\":\"Patient\",\"id\":\"empty-structure\"}");
  }

  @Test
  void omitsAnArrayWhoseEveryElementIsNull() {
    assertSerialises(
        RowFactory.create("Patient", "null-array", Arrays.asList(null, null), null),
        "{\"resourceType\":\"Patient\",\"id\":\"null-array\"}");
  }

  @Test
  void omitsAnArrayWhoseEveryElementBecomesEmpty() {
    assertSerialises(
        RowFactory.create(
            "Patient", "empty-elements", List.of(RowFactory.create(null, null)), null),
        "{\"resourceType\":\"Patient\",\"id\":\"empty-elements\"}");
  }

  @Test
  void keepsWhatIsPresentBesideWhatIsOmitted() {
    assertSerialises(
        RowFactory.create(
            "Patient",
            "mixed",
            Arrays.asList(RowFactory.create(null, null), RowFactory.create("Smith", null)),
            RowFactory.create((Object) null)),
        "{\"resourceType\":\"Patient\",\"id\":\"mixed\",\"name\":[{\"family\":\"Smith\"}]}");
  }

  private static void assertSerialises(@Nonnull final Row row, @Nonnull final String expected) {
    final Dataset<Row> stored = TransformFixtures.spark().createDataFrame(List.of(row), SCHEMA);
    final String actual =
        ResourceSerialiser.of(TransformFixtures.DEFINITIONS)
            .serialise("Patient", stored)
            .collectAsList()
            .get(0);
    SemanticJson.difference("Patient", SemanticJson.parse(expected), SemanticJson.parse(actual))
        .ifPresent(difference -> fail("The output was not as required at " + difference));
  }

  @Nonnull
  private static StructField field(
      @Nonnull final String name, @Nonnull final org.apache.spark.sql.types.DataType type) {
    return new StructField(name, type, true, Metadata.empty());
  }
}
