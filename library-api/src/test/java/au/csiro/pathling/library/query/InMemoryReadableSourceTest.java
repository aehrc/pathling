package au.csiro.pathling.library.query;

import au.csiro.pathling.config.QueryConfiguration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.BeforeAll;

public class InMemoryReadableSourceTest extends BaseReadableSourceTest {

  /**
   * Set up InMemory client.
   */
  @BeforeAll
  public static void setupClient() {

    final Dataset<Row> patientJsonDf = spark.read()
        .text(FHIR_JSON_DATA_PATH.resolve("Patient.ndjson").toString());
    final Dataset<Row> conditionJsonDf = spark.read()
        .text(FHIR_JSON_DATA_PATH.resolve("Condition.ndjson").toString());

    readableSource = pathlingCtx.datasources()
        .directBuilder()
        .withQueryConfiguration(QueryConfiguration.builder().explainQueries(true).build())
        .withResource(ResourceType.PATIENT,
            pathlingCtx.encode(patientJsonDf, "Patient"))
        .withResource(ResourceType.CONDITION,
            pathlingCtx.encode(conditionJsonDf, "Condition"))
        .build();
  }
}
