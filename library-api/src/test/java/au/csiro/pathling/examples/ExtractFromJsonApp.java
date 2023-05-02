package au.csiro.pathling.examples;

import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import java.nio.file.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

public class ExtractFromJsonApp {

  public static void main(final String[] args) {

    final Path fhirData = Path.of("fhir-server/src/test/resources/test-data/fhir").toAbsolutePath();
    System.out.printf("JSON Data: %s\n", fhirData);

    final SparkSession spark = SparkSession.builder()
        .appName(ExtractFromJsonApp.class.getName())
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate();

    final PathlingContext ptc = PathlingContext.create(spark);

    final QueryableDataSource data = ptc.read()
        .ndjson(fhirData.toUri().toString());

    final Dataset<Row> patientResult = data.extract(ResourceType.PATIENT)
        .column("id")
        .column("gender")
        .column("reverseResolve(Condition.subject).code.coding.code")
        .filter("gender = 'male'")
        .execute()
        .limit(5);

    patientResult.show(5);

    final Dataset<Row> conditionResult = data.extract(ResourceType.CONDITION)
        .column("id")
        .column("code.coding.code", "code")
        .column("code.coding.display", "display_name")
        .column("subject.resolve().ofType(Patient).gender", "patient_gender")
        .execute()
        .limit(5);

    conditionResult.show(5);
  }
}
