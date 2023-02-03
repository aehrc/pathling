package au.csiro.pathling.library.query;

import au.csiro.pathling.config.QueryConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.TestHelpers;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.test.assertions.DatasetAssert;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

public class PathlingClientTest {

  private static final Path FHIR_JSON_DATA_PATH = Path.of(
      "../fhir-server/src/test/resources/test-data/fhir").toAbsolutePath();
  private static SparkSession spark;
  private static PathlingClient pathlingClient;

  /**
   * Set up Spark.
   */
  @BeforeAll
  public static void setUpAll() {
    spark = TestHelpers.spark();

    final TerminologyServiceFactory terminologyServiceFactory = mock(
        TerminologyServiceFactory.class, withSettings().serializable());

    final PathlingContext ptc = PathlingContext.create(spark, FhirEncoders.forR4().getOrCreate(),
        terminologyServiceFactory);

    final Dataset<Row> patientJsonDf = spark.read()
        .text(FHIR_JSON_DATA_PATH.resolve("Patient.ndjson").toString());
    final Dataset<Row> conditionJsonDf = spark.read()
        .text(FHIR_JSON_DATA_PATH.resolve("Condition.ndjson").toString());

    pathlingClient = ptc.newClientBuilder()
        .withQueryConfiguration(QueryConfiguration.builder().explainQueries(true).build())
        .withResource(ResourceType.PATIENT, ptc.encode(patientJsonDf, "Patient").cache())
        .withResource(ResourceType.CONDITION, ptc.encode(conditionJsonDf, "Condition").cache())
        .build();
  }

  /**
   * Tear down Spark.
   */
  @AfterAll
  public static void tearDownAll() {
    spark.stop();
  }


  @Test
  public void testExtractQueryBound() {
    final Dataset<Row> patientResult = pathlingClient.newExtractQuery(ResourceType.PATIENT)
        .withColumn("id")
        .withColumn("gender")
        .withColumn("reverseResolve(Condition.subject).code.coding")
        .withFilter("gender = 'male'")
        .withFilter("reverseResolve(Condition.subject).recordedDate.first() < @2010-06-19")
        .execute();

    assertEquals(List.of("id", "gender", "reverseResolve(Condition.subject).code.coding"),
        Arrays.asList(patientResult.columns()));

    DatasetAssert.of(patientResult)
        .hasRows(spark, "results/PathlingClientTest/testExtractQueryBound.csv");
  }

  @Test
  public void testExtractQueryUnbound() {
    final Dataset<Row> conditionResult = ExtractQuery.of(ResourceType.CONDITION)
        .withColumn("id")
        .withColumn("code.coding.code", "code")
        .withColumn("code.coding.display", "display")
        .withColumn("subject.resolve().ofType(Patient).gender", "patientGender")
        .withLimit(5)
        .execute(pathlingClient);

    assertEquals(List.of("id", "code", "display",
            "patientGender"),
        Arrays.asList(conditionResult.columns()));

    DatasetAssert.of(conditionResult)
        .hasRows(spark, "results/PathlingClientTest/testExtractQueryUnbound.csv");
  }
}
