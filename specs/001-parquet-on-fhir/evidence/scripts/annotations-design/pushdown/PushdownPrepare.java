import au.csiro.pathling.library.PathlingContext;
import java.io.File;
import java.util.Arrays;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

/**
 * Encodes Patient NDJSON through the library API (annotations on by default), then rewrites it
 * sorted by birthDate into small row groups: the full engine layout, and narrow hand-built
 * variants (flat INT64 micros, flat INT96, grouped bounds) of id, birthDate and _birthDate.
 *
 * <p>Usage: PushdownPrepare [ndjson dir] [work dir] [block size bytes]
 */
public class PushdownPrepare {
  public static void main(final String[] args) {
    final String block = args[2];
    final SparkSession spark =
        SparkSession.builder()
            .master("local[2]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
            .config("spark.hadoop.parquet.block.size", block)
            .config("spark.hadoop.parquet.block.size.row.check.min", "100")
            .config("spark.hadoop.parquet.block.size.row.check.max", "1000")
            .config("spark.sql.shuffle.partitions", "8")
            .getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");
    final PathlingContext ptc = PathlingContext.create(spark);
    final String work = args[1];
    if (!new File(work + "/encoded").exists()) {
      ptc.read().ndjson(args[0]).write().parquet(work + "/encoded");
    }
    System.out.println("ENCODED ENTRIES " + Arrays.toString(new File(work + "/encoded").list()));
    // The write is a set of Patient.NNNNN-<id>.parquet files, all of which hold Patients.
    final Dataset<Row> enc = spark.read().parquet(work + "/encoded/Patient.*.parquet");
    System.out.println("ENCODED ROWS " + enc.count());
    enc.printSchema();
    enc.sort("birthDate").coalesce(1).write().mode("overwrite").parquet(work + "/full");
    final Dataset<Row> full = spark.read().parquet(work + "/full");
    final Dataset<Row> narrow = full.select("id", "birthDate", "_birthDate");
    narrow.coalesce(1).write().mode("overwrite").parquet(work + "/narrow_micros");
    final Dataset<Row> grouped =
        full.select(
            functions.col("id"),
            functions.col("birthDate"),
            functions
                .struct(
                    functions
                        .struct(
                            functions.col("_birthDate.__start").alias("lower"),
                            functions.col("_birthDate.__end").alias("upper"))
                        .alias("__bounds"))
                .alias("_birthDate"));
    grouped.coalesce(1).write().mode("overwrite").parquet(work + "/narrow_grouped");
    spark.conf().set("spark.sql.parquet.outputTimestampType", "INT96");
    narrow.coalesce(1).write().mode("overwrite").parquet(work + "/narrow_int96");
    spark.stop();
  }
}
