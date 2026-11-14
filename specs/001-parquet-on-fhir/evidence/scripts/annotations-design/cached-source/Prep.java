/**
 * Encodes the NDJSON through the library, splices the metadata in (Splice.java), and writes
 * Parquet for each variant. Usage: Prep <ndjson dir> <out dir> [Patient,Encounter].
 */
import au.csiro.pathling.library.PathlingContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Prep {

  public static void main(final String[] args) {
    final SparkSession spark =
        SparkSession.builder()
            .master("local[4]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
            .getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");
    final PathlingContext ptc = PathlingContext.create(spark);
    final String[] types = args.length > 2 ? args[2].split(",") : new String[] {"Patient", "Encounter"};
    for (final String type : types) {
      final Dataset<Row> encoded = ptc.read().ndjson(args[0]).read(type);
      for (final boolean ext : new boolean[] {false, true}) {
        final String v = ext ? "ext" : "plain";
        final String out = args[1] + "/" + v + "/" + type + ".parquet";
        Splice.apply(encoded, type, ext).write().mode("overwrite").parquet(out);
        final Dataset<Row> back = spark.read().parquet(out);
        System.out.println("WROTE " + v + " " + type + " rows=" + back.count());
        if (ext) {
          System.out.println(back.schema().treeString());
        }
      }
    }
    spark.stop();
  }
}
