import au.csiro.pathling.library.PathlingContext;
import ca.uhn.fhir.parser.IParser;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseResource;

/**
 * Usage: WriteLayouts <Observation.ndjson> <outputDir>. Writes the Observations to Parquet in both
 * layouts: <outputDir>/pof/Observation through PathlingContext.encode, which writes the Parquet on
 * FHIR layout since the switch, and <outputDir>/previous/Observation through the previous layout's
 * encoders, which PathlingContext no longer writes, reached with getFhirEncoders().
 */
public class WriteLayouts {
  public static void main(String[] args) throws Exception {
    final SparkSession spark = SparkSession.builder().master("local[4]")
        .config("spark.ui.enabled", "false").getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");
    final PathlingContext pc = PathlingContext.create(spark);
    final List<String> lines = Files.readAllLines(Path.of(args[0]));

    final Dataset<String> json = spark.createDataset(lines, Encoders.STRING());
    pc.encode(json.toDF(), "Observation").write().parquet(args[1] + "/pof/Observation");

    final IParser parser = pc.getFhirEncoders().getContext().newJsonParser();
    final List<IBaseResource> resources =
        lines.stream().map(line -> (IBaseResource) parser.parseResource(line)).toList();
    final Dataset<IBaseResource> previous =
        spark.createDataset(resources, pc.getFhirEncoders().of("Observation"));
    previous.toDF().write().parquet(args[1] + "/previous/Observation");

    for (final String layout : List.of("pof", "previous")) {
      final Dataset<Row> read = spark.read().parquet(args[1] + "/" + layout + "/Observation");
      System.out.println("WROTE " + layout + " rows=" + read.count()
          + " columns=" + read.columns().length);
    }
    spark.stop();
  }
}
