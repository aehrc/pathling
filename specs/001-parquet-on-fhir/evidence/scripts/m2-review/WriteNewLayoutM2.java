import au.csiro.pathling.definition.fhir.FhirDefinitionContext;
import au.csiro.pathling.io.json.FhirJsonReader;
import au.csiro.pathling.io.transform.ResourceTransformer;
import ca.uhn.fhir.context.FhirContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Usage: WriteNewLayoutM2 <Observation.ndjson> <outputDir>. Writes the Observations to
 * <outputDir>/pof/Observation in the Parquet on FHIR layout through the io module's reader as it
 * was before decision 82's redesign, at 633894edfc, when PathlingContext did not yet write the
 * layout. It compiles against the jars of that commit only (T100o).
 */
public class WriteNewLayoutM2 {
  public static void main(final String[] args) {
    final SparkSession spark =
        SparkSession.builder().master("local[4]").config("spark.ui.enabled", "false").getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");
    final FhirJsonReader reader =
        FhirJsonReader.of(
            spark, ResourceTransformer.of(FhirDefinitionContext.of(FhirContext.forR4())));
    reader.read("Observation", args[0]).write().parquet(args[1] + "/pof/Observation");
    final Dataset<Row> read = spark.read().parquet(args[1] + "/pof/Observation");
    System.out.println("WROTE pof rows=" + read.count() + " columns=" + read.columns().length);
    spark.stop();
  }
}
