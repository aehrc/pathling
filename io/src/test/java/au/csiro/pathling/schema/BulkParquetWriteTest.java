package au.csiro.pathling.schema;

import au.csiro.pathling.test.TestResources;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BulkParquetWriteTest {

  Path tempDirectory;

  @BeforeEach
  void setUp() throws IOException {
    tempDirectory = Files.createTempDirectory("pathling-BulkParquetWriteTest");
  }

  @Test
  void test() {
    SparkSession.builder()
        .master("local[*]")
        .getOrCreate();
    final FhirJsonReader reader = new FhirJsonReader("ExplanationOfBenefit", "R4",
        Map.of("multiLine", "false")
    );

    final URL testData = TestResources.getResourceAsUrl(
        "bulk/fhir/ExplanationOfBenefit.ndjson");
    final Dataset<Row> data = reader.read(testData.toString());
    data.write()
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .save(tempDirectory.resolve("parquet/ExplanationOfBenefit.parquet").toString());
  }

  @AfterEach
  void tearDown() throws IOException {
    TestResources.deleteRecursively(tempDirectory);
  }

}
