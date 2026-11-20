/**
 * Ticket 12: writes Observations to Parquet in the Parquet on FHIR layout twice, through
 * PathlingContext.encode with the canonical annotation on (the default) and with it off.
 *
 * <p>Usage: WriteAnnotation <Observation.ndjson> <out> [copies]. With copies above 1 each resource
 * is written that many times, with its id suffixed, for the measurements that need the per-row
 * cost to show.
 */
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.schema.SchemaConfiguration;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class WriteAnnotation {
  public static void main(final String[] args) throws Exception {
    final SparkSession spark =
        SparkSession.builder().master("local[4]").config("spark.ui.enabled", "false").getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");
    final List<String> lines = Files.readAllLines(Path.of(args[0]));
    final int copies = args.length > 2 ? Integer.parseInt(args[2]) : 1;
    final Dataset<String> json = spark.createDataset(lines, Encoders.STRING());
    for (final boolean annotated : List.of(true, false)) {
      final PathlingContext pc =
          PathlingContext.builder(spark)
              .schemaConfiguration(
                  SchemaConfiguration.builder().enableCanonicalAnnotation(annotated).build())
              .build();
      Dataset<Row> encoded = pc.encode(json.toDF(), "Observation");
      if (copies > 1) {
        encoded =
            encoded
                .crossJoin(spark.range(copies).toDF("copy"))
                .withColumn("id", functions.concat_ws("-", functions.col("id"), functions.col("copy")))
                .drop("copy");
      }
      final String out = args[1] + "/" + (annotated ? "annotated" : "plain") + "/Observation";
      encoded.write().mode("overwrite").parquet(out);
      final Dataset<Row> read = spark.read().parquet(out);
      System.out.println(
          "WROTE "
              + (annotated ? "annotated" : "plain")
              + " rows="
              + read.count()
              + " valueQuantity="
              + read.schema().apply("valueQuantity").dataType().simpleString());
    }
    spark.stop();
  }
}
