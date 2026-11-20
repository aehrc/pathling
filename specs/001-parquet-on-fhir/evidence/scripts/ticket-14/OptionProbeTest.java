/**
 * Ticket 14, owner decision 2 of 2026-10-27: whether the Parquet timestamp type can be set for one
 * write. Writes a timestamp column with each candidate writer option, to plain Parquet and to Delta,
 * and once with the session setting changed for the duration of a Delta merge, and prints each
 * footer's type. Run as a JUnit test from library-api's test sources, as RangeDeltaProbeTest is.
 *
 * <p>Output on Spark 4.0.2 (Delta as bundled):
 *
 * <pre>
 * parquet option spark.sql.parquet.outputTimestampType: required int96 t
 * delta option spark.sql.parquet.outputTimestampType: optional int96 t
 * parquet option outputTimestampType: required int96 t
 * delta option outputTimestampType: optional int96 t
 * parquet option parquet.outputTimestampType: required int96 t
 * delta option parquet.outputTimestampType: optional int96 t
 * delta merge scoped session: optional int64 t (TIMESTAMP(MICROS,true)) | optional int96 t
 * session after: INT96
 * </pre>
 *
 * <p>The merge's new file is int64 and the first write's is int96: no writer option changes the
 * type, and the session setting does, for the writes made while it is set.
 */
package au.csiro.pathling.library.scratch;

import au.csiro.pathling.library.TestHelpers;
import io.delta.tables.DeltaTable;
import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

class OptionProbeTest {

  static String footers(final File dir) throws Exception {
    final StringBuilder b = new StringBuilder();
    for (final File f :
        Arrays.stream(dir.listFiles()).filter(f -> f.getName().endsWith(".parquet")).toList()) {
      try (final ParquetFileReader r =
          ParquetFileReader.open(
              HadoopInputFile.fromPath(new Path(f.getPath()), new Configuration()))) {
        b.append(r.getFooter().getFileMetaData().getSchema().getType("t").toString().trim())
            .append(" | ");
      }
    }
    return b.toString();
  }

  @Test
  void probe() throws Exception {
    final SparkSession spark = TestHelpers.spark();
    spark.conf().unset("spark.sql.parquet.outputTimestampType");
    final Dataset<Row> df = spark.sql("select 1 as id, timestamp'2020-01-01 00:00:00' as t");
    final String key = "spark.sql.parquet.outputTimestampType";
    final File base = Files.createTempDirectory("optprobe").toFile();
    for (final String opt : new String[] {key, "outputTimestampType", "parquet.outputTimestampType"}) {
      final File p = new File(base, "p-" + opt);
      df.write().option(opt, "TIMESTAMP_MICROS").parquet(p.getPath());
      System.out.println("PROBE parquet option " + opt + ": " + footers(p));
      final File d = new File(base, "d-" + opt);
      df.write().format("delta").option(opt, "TIMESTAMP_MICROS").save(d.getPath());
      System.out.println("PROBE delta option " + opt + ": " + footers(d));
    }
    final File d = new File(base, "d-merge");
    df.write().format("delta").save(d.getPath());
    spark.conf().set(key, "TIMESTAMP_MICROS");
    try {
      DeltaTable.forPath(spark, d.getPath())
          .as("a")
          .merge(spark.sql("select 2 as id, timestamp'2021-01-01 00:00:00' as t").as("b"), "a.id = b.id")
          .whenNotMatched()
          .insertAll()
          .execute();
    } finally {
      spark.conf().unset(key);
    }
    System.out.println("PROBE delta merge scoped session: " + footers(d));
    System.out.println("PROBE session after: " + spark.conf().get(key));
  }
}
