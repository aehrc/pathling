package au.csiro.pathling.library.query;

import au.csiro.pathling.config.QueryConfiguration;
import au.csiro.pathling.config.StorageConfiguration;
import au.csiro.pathling.io.Database;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.BeforeAll;

public class DatabasePathlingClientTest extends BasePathlingClientTest {
  
  @BeforeAll
  public static void setupClient() throws IOException {

    final Dataset<Row> patientJsonDf = spark.read()
        .text(FHIR_JSON_DATA_PATH.resolve("Patient.ndjson").toString());
    final Dataset<Row> conditionJsonDf = spark.read()
        .text(FHIR_JSON_DATA_PATH.resolve("Condition.ndjson").toString());

    // TODO: Possibly refer back to imported test data when it's available as dependency for this module.
    // Import test data to temporary database
    final File tempDatabasePath = Files.createTempDirectory("pathling-database").toFile();
    tempDatabasePath.deleteOnExit();
    final String databaseUri = tempDatabasePath.toURI().toString();

    final Database database = new Database(
        StorageConfiguration.forDatabase(databaseUri, "default"),
        spark, pathlingCtx.getFhirEncoders());

    database.overwrite(ResourceType.PATIENT, pathlingCtx.encode(patientJsonDf, "Patient"));
    database.overwrite(ResourceType.CONDITION, pathlingCtx.encode(conditionJsonDf, "Condition"));

    pathlingClient = pathlingCtx.newClientBuilder()
        .database()
        .withQueryConfiguration(QueryConfiguration.builder().explainQueries(true).build())
        .withStorageConfiguration(StorageConfiguration.forDatabase(databaseUri, "default"))
        .build();
  }
}
