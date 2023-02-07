package au.csiro.pathling.library.query;

import au.csiro.pathling.config.QueryConfiguration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.BeforeAll;

public class InMemoryPathlingClientTest extends BasePathlingClientTest {

  /**
   * Set up InMemory client.
   */
  @BeforeAll
  public static void setupClient() {

    final Dataset<Row> patientJsonDf = spark.read()
        .text(FHIR_JSON_DATA_PATH.resolve("Patient.ndjson").toString());
    final Dataset<Row> conditionJsonDf = spark.read()
        .text(FHIR_JSON_DATA_PATH.resolve("Condition.ndjson").toString());

    pathlingClient = pathlingCtx.newClientBuilder()
        .inMemory()
        .withQueryConfiguration(QueryConfiguration.builder().explainQueries(true).build())
        .withResource(ResourceType.PATIENT,
            pathlingCtx.encode(patientJsonDf, "Patient").cache())
        .withResource(ResourceType.CONDITION,
            pathlingCtx.encode(conditionJsonDf, "Condition").cache())
        .build();
  }
}
