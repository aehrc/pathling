package au.csiro.pathling.examples;

import au.csiro.pathling.config.QueryConfiguration;
import au.csiro.pathling.config.StorageConfiguration;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.query.ExtractQuery;
import au.csiro.pathling.library.query.PathlingClient;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import java.nio.file.Path;

public class ExtractFromDatabaseApp {


  public static void main(String[] args) {

    final String warehouseUrl = Path.of("fhir-server/src/test/resources/test-data").toAbsolutePath()
        .toUri().toString();
    System.out.printf("Warehouse URL: %s\n", warehouseUrl);

    final SparkSession spark = SparkSession.builder()
        .appName(ExtractFromJsonApp.class.getName())
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate();

    final PathlingContext ptc = PathlingContext.create(spark);

    final PathlingClient pathlingClient = ptc.newClientBuilder()
        .database()
        .withQueryConfiguration(QueryConfiguration.builder().explainQueries(true).build())
        .withStorageConfiguration(StorageConfiguration.forDatabase(warehouseUrl, "parquet"))
        .build();

    final Dataset<Row> patientResult = pathlingClient.newExtractQuery(ResourceType.PATIENT)
        .withColumn("id")
        .withColumn("gender")
        .withColumn("reverseResolve(Condition.subject).code.coding.code")
        .withFilter("gender = 'male'")
        .withLimit(10)
        .execute();

    patientResult.show(5);

    final Dataset<Row> conditionResult = ExtractQuery.of(ResourceType.CONDITION)
        .withColumn("id")
        .withColumn("code.coding.code", "code")
        .withColumn("code.coding.display", "display_name")
        .withColumn("subject.resolve().ofType(Patient).gender", "patient_gender")
        .withLimit(10)
        .execute(pathlingClient);

    conditionResult.show(5);
  }
}
