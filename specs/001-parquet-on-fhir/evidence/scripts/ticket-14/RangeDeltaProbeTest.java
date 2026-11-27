/**
 * Ticket 14: the type the range annotations are stored at, against the Parquet on FHIR
 * specification's int96 with TIMESTAMP(isAdjustedToUTC=true, unit=MILLIS). Encodes three Patients
 * through PathlingContext, which annotates them, writes them as Delta and as Parquet under each
 * value of spark.sql.parquet.outputTimestampType, prints the footer's type of _birthDate.__start
 * and __end, and evaluates two comparisons over what is read back, so that what each type does to
 * the engine's results is shown. The years 1850 and 1500 show whether Spark's default rebase modes
 * accept dates before 1900 and before the Gregorian cutover.
 *
 * <p>It runs as a JUnit test from library-api's test sources, whose Spark session has Delta:
 * copy it to library-api/src/test/java/au/csiro/pathling/library/scratch/ and run
 * {@code mvn test -pl library-api -Dtest=RangeDeltaProbeTest}. The output of a run on Spark 4.0.2
 * is in range-type-probe.txt beside it.
 */
package au.csiro.pathling.library.scratch;

import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.TestHelpers;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

class RangeDeltaProbeTest {

  @Test
  void probe() throws Exception {
    final SparkSession spark = TestHelpers.spark();
    final PathlingContext pc = PathlingContext.create(spark);
    final List<String> json =
        List.of(
            "{\"resourceType\":\"Patient\",\"id\":\"a\",\"birthDate\":\"1980\"}",
            "{\"resourceType\":\"Patient\",\"id\":\"b\",\"birthDate\":\"1850-01-01\"}",
            "{\"resourceType\":\"Patient\",\"id\":\"c\",\"birthDate\":\"1500-02-03\"}");
    final Dataset<Row> encoded =
        pc.encode(spark.createDataset(json, Encoders.STRING()).toDF(), "Patient");
    System.out.println(
        "PROBE conf outputTimestampType="
            + spark.conf().getOption("spark.sql.parquet.outputTimestampType"));
    final String out = System.getProperty("java.io.tmpdir");
    for (final String type : List.of("INT96", "TIMESTAMP_MICROS", "TIMESTAMP_MILLIS")) {
      spark.conf().set("spark.sql.parquet.outputTimestampType", type);
      for (final String format : List.of("delta", "parquet")) {
        final File dir = new File(out, format + "-" + type);
        try {
          encoded.write().mode("overwrite").format(format).save(dir.getPath());
          final File part =
              Arrays.stream(dir.listFiles())
                  .filter(f -> f.getName().endsWith(".parquet"))
                  .findFirst()
                  .orElseThrow();
          try (final ParquetFileReader reader =
              ParquetFileReader.open(
                  HadoopInputFile.fromPath(new Path(part.getPath()), new Configuration()))) {
            System.out.println(
                "PROBE "
                    + format
                    + " "
                    + type
                    + " footer: "
                    + reader
                        .getFooter()
                        .getFileMetaData()
                        .getSchema()
                        .getType("_birthDate")
                        .toString()
                        .replace('\n', ' '));
          }
          final Dataset<Row> back = spark.read().format(format).load(dir.getPath());
          back.select(
                  back.col("id"),
                  back.col("_birthDate.__end"),
                  pc.fhirPathToColumn("Patient", "birthDate >= @1980").alias("ge1980"),
                  pc.fhirPathToColumn("Patient", "birthDate <= @1500-02-03").alias("le1500"))
              .collectAsList()
              .forEach(
                  row ->
                      System.out.println(
                          "PROBE "
                              + format
                              + " "
                              + type
                              + " "
                              + row.getString(0)
                              + " end="
                              + row.getTimestamp(1).toInstant()
                              + " birthDate>=@1980 -> "
                              + row.get(2)
                              + " birthDate<=@1500-02-03 -> "
                              + row.get(3)));
        } catch (final Exception e) {
          System.out.println("PROBE " + format + " " + type + " FAILED: " + e);
        }
      }
    }
    spark.conf().unset("spark.sql.parquet.outputTimestampType");
  }
}
