package au.csiro.pathling.library.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.TestHelpers;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.test.assertions.DatasetAssert;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

abstract public class BasePathlingClientTest {

  protected static final Path FHIR_JSON_DATA_PATH = Path.of(
      "../fhir-server/src/test/resources/test-data/fhir").toAbsolutePath();
  protected static PathlingContext pathlingCtx;
  protected static SparkSession spark;

  protected static PathlingClient pathlingClient;

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


  @Test
  public void testAggregateQuery() {
    final Dataset<Row> patientResult = pathlingClient.newAggregateQuery(ResourceType.PATIENT)
        .withGrouping("gender")
        .withGrouping("maritalStatus.coding.code")
        .withAggregation("count()")
        .withFilter("birthDate > @1957-06-06")
        .execute();

    // TODO: Enable this assertion once we have a way to get the column names from the query.
    // assertEquals(List.of("id", "gender", "reverseResolve(Condition.subject).code.coding"),
    //     Arrays.asList(patientResult.columns()));

    DatasetAssert.of(patientResult)
        .debugAllRows()
        .hasRows(spark, "results/PathlingClientTest/testAggregateQuery.csv");
  }
}
