/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.library.data;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.library.FhirMimeTypes;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.TestHelpers;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.test.assertions.DatasetAssert;
import java.nio.file.Path;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class DataSourcesTest {

  protected static final Path TEST_DATA_PATH = Path.of(
      "src/test/resources/test-data").toAbsolutePath().normalize();

  protected static final Path TEST_JSON_DATA_PATH = TEST_DATA_PATH.resolve("ndjson");
  protected static final Path TEST_DELTA_DATA_PATH = TEST_DATA_PATH.resolve("delta");

  protected static PathlingContext pathlingCtx;
  protected static SparkSession spark;

  /**
   * Set up Spark.
   */
  @BeforeAll
  public static void setupContext() {
    spark = TestHelpers.spark();

    final TerminologyServiceFactory terminologyServiceFactory = mock(
        TerminologyServiceFactory.class, withSettings().serializable());

    pathlingCtx = PathlingContext.create(spark, FhirEncoders.forR4().getOrCreate(),
        terminologyServiceFactory);
  }

  /**
   * Tear down Spark.
   */
  @AfterAll
  public static void tearDownAll() {
    spark.stop();
  }

  public static Stream<Arguments> dataSources() {
    final Dataset<Row> patientJsonDf = spark.read()
        .text(TEST_JSON_DATA_PATH.resolve("Patient.ndjson").toString());
    final Dataset<Row> conditionJsonDf = spark.read()
        .text(TEST_JSON_DATA_PATH.resolve("Condition.ndjson").toString());

    return Stream.of(
        Arguments.of("with directBuilder()", pathlingCtx.datasources()
            .directBuilder()
            .withResource(ResourceType.PATIENT,
                pathlingCtx.encode(patientJsonDf, "Patient"))
            .withResource("Condition",
                pathlingCtx.encode(conditionJsonDf, "Condition"))
            .build()
        ),
        Arguments.arguments("with filesystemBuilder()",
            pathlingCtx.datasources().filesystemBuilder()
                .withFilesGlob(TEST_JSON_DATA_PATH.resolve("*.ndjson").toString())
                .withReader(spark.read().format("text"))
                .withFilepathMapper(SupportFunctions::basenameToResource)
                .withDatasetTransformer(
                    SupportFunctions.textEncodingTransformer(pathlingCtx, FhirMimeTypes.FHIR_JSON))
                .build()
        ),
        Arguments.arguments("with databaseBuilder()",
            pathlingCtx.datasources().databaseBuilder()
                .withWarehouseUrl(TEST_DATA_PATH.toUri().toString())
                .withDatabaseName("delta")
                .build()
        ),
        Arguments.arguments("fromFiles with reader='delta'",
            pathlingCtx.datasources().fromFiles(
                TEST_DELTA_DATA_PATH.resolve("*.parquet").toString(),
                SupportFunctions::basenameToResource, spark.read().format("delta"))
        ),
        Arguments.arguments("fromFiles with format='delta'",
            pathlingCtx.datasources().fromFiles(
                TEST_DELTA_DATA_PATH.resolve("*.parquet").toString(),
                SupportFunctions::basenameToResource, "delta")
        ),
        Arguments.arguments("fromTextFiles with mimeType='ndjson'",
            pathlingCtx.datasources()
                .fromTextFiles(TEST_JSON_DATA_PATH.resolve("*.ndjson").toUri().toString(),
                    SupportFunctions::basenameToResource,
                    FhirMimeTypes.FHIR_JSON)
        ),
        Arguments.arguments("fromWarehouse",
            pathlingCtx.datasources().fromWarehouse(TEST_DATA_PATH.toUri().toString(), "delta")
        ),
        Arguments.of("fromNdjsonDir", pathlingCtx.datasources()
            .fromNdjsonDir(TEST_JSON_DATA_PATH.toUri().toString())),
        Arguments.arguments("fromParquetDir",
            pathlingCtx.datasources().fromParquetDir(TEST_DELTA_DATA_PATH.toUri().toString())
        )
    );
  }

  @ParameterizedTest(name = "DataSource: {0}")
  @MethodSource("dataSources")
  public void testBuildsCorrectDataSource(final @Nonnull String name,
      final @Nonnull ReadableSource readableSource) {
    final Dataset<Row> patientCount = readableSource.aggregate(ResourceType.PATIENT)
        .withAggregation("count()").execute();
    DatasetAssert.of(patientCount).hasRows(RowFactory.create(9));

    final Dataset<Row> conditionCount = readableSource.aggregate("Condition")
        .withAggregation("count()").execute();
    DatasetAssert.of(conditionCount).hasRows(RowFactory.create(71));
  }
}
