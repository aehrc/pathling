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
package au.csiro.pathling.library.io;

import static au.csiro.pathling.library.io.LayoutFixtures.newPatients;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.fhirpath.encoding.CodingSchema;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.TestHelpers;
import au.csiro.pathling.library.io.source.LayoutDetector;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.sql.udf.TranslateUdf;
import jakarta.annotation.Nonnull;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.r4.model.Coding;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests that resource data carrying the results of a terminology function, written through a sink,
 * is read back through the matching source (decision 100).
 *
 * <p>The terminology functions build codings through {@link CodingSchema}, which still carries the
 * previous layout's {@code _fid} field. While any previous-layout marker classified a table, such
 * data was refused as the previous layout when it was read back, so Pathling rejected its own
 * output. The codings here are built as {@code translate} returns them, with its return type.
 *
 * @author Piotr Szul
 */
class TerminologyResultRoundTripTest {

  @TempDir static Path root;

  private static SparkSession spark;
  private static PathlingContext context;

  /** Set up Spark, with a warehouse in a temporary directory, and the Pathling context. */
  @BeforeAll
  static void setUp() {
    spark =
        TestHelpers.sparkBuilder()
            .config("spark.sql.warehouse.dir", root.resolve("spark-warehouse").toString())
            .getOrCreate();
    context = PathlingContext.create(spark);
  }

  /** Stop Spark. */
  @AfterAll
  static void tearDown() {
    spark.stop();
  }

  /**
   * Patients carrying a translation, written through each sink and read back through the matching
   * source, are accepted and keep the translation.
   */
  @ParameterizedTest
  @ValueSource(strings = {"parquet", "delta", "tables"})
  void dataCarryingTerminologyResultsIsReadBack(@Nonnull final String format) {
    final Dataset<Row> patients = withTranslation(newPatients(context, "n1"));
    // The data does carry the coding's _fid, so the round trip tests what it is meant to.
    assertTrue(
        LayoutDetector.detect(patients.schema())
            .getPreviousLayoutMarkers()
            .contains("translated._fid"));

    final Dataset<Row> readBack = roundTrip(format, patients).read("Patient");

    assertEquals(1, readBack.count());
    final List<Row> translated = readBack.select("translated").first().getList(0);
    assertEquals("http://snomed.info/sct", translated.get(0).getAs("system"));
    assertEquals("444814009", translated.get(0).getAs("code"));
  }

  /** Adds a column holding a translation, as the translate function returns it. */
  @Nonnull
  private static Dataset<Row> withTranslation(@Nonnull final Dataset<Row> patients) {
    final Coding coding = new Coding("http://snomed.info/sct", "444814009", "Viral sinusitis");
    final StructType schema =
        new StructType()
            .add("id", DataTypes.StringType)
            .add("translated", TranslateUdf.RETURN_TYPE);
    final Dataset<Row> translations =
        spark.createDataFrame(
            List.of(RowFactory.create("n1", CodingSchema.encodeListToArray(List.of(coding)))),
            schema);
    return patients.join(translations, "id");
  }

  /** Writes the data through the sink of the given format and opens the matching source. */
  @Nonnull
  private static QueryableDataSource roundTrip(
      @Nonnull final String format, @Nonnull final Dataset<Row> patients) {
    final var write = context.read().datasets().dataset("Patient", patients).write();
    final String location = root.resolve(UUID.randomUUID().toString()).toString();
    return switch (format) {
      case "parquet" -> {
        write.parquet(location);
        yield context.read().parquet(location);
      }
      case "delta" -> {
        write.delta(location);
        yield context.read().delta(location);
      }
      default -> {
        final String database = "db_" + UUID.randomUUID().toString().replace("-", "");
        spark.sql("CREATE DATABASE " + database);
        write.tables(database);
        yield context.read().tables(database);
      }
    };
  }
}
