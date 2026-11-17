/**
 * Writes Patients to Parquet twice, with the range annotations on and off, multiplied to the
 * requested number of copies. Usage: WriteData <Patient.ndjson> <out> <copies>.
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

public class WriteData {

  /**
   * Adds a stored {@code _given} to every name, aligned with {@code given} by position, with an
   * extension on about one given in ten and null elsewhere, since the encoder does not write the
   * groups of a plain string. The engine does not read it; the hand-built arms do.
   */
  private static final String GIVEN_META =
      "transform(name, n -> named_struct('use', n.use, 'family', n.family, 'given', n.given,"
          + " 'period', n.period, '_given', transform(n.given, g -> if(pmod(hash(g), 10) = 0,"
          + " named_struct('id', cast(null as string), 'extension', array(named_struct('url',"
          + " 'http://x', 'valueString', 'x'))), cast(null as struct<id:string,"
          + " extension:array<struct<url:string,valueString:string>>>)))))";

  public static void main(final String[] args) throws Exception {
    final SparkSession spark =
        SparkSession.builder().master("local[4]").config("spark.ui.enabled", "false").getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");
    final List<String> lines = Files.readAllLines(Path.of(args[0]));
    final int copies = Integer.parseInt(args[2]);
    final Dataset<String> json = spark.createDataset(lines, Encoders.STRING());
    for (final boolean annotated : List.of(true, false)) {
      final PathlingContext pc =
          PathlingContext.builder(spark)
              .schemaConfiguration(
                  SchemaConfiguration.builder().enableRangeAnnotation(annotated).build())
              .build();
      final Dataset<Row> encoded =
          pc.encode(json.toDF(), "Patient")
              .crossJoin(spark.range(copies).toDF("copy"))
              .withColumn("id", functions.concat_ws("-", functions.col("id"), functions.col("copy")))
              .drop("copy")
              .withColumn("name", functions.expr(GIVEN_META))
              .repartition(8);
      final String out = args[1] + "/" + (annotated ? "annotated" : "plain") + "/Patient.parquet";
      encoded.write().mode("overwrite").parquet(out);
      final Dataset<Row> read = spark.read().parquet(out);
      System.out.println("WROTE " + (annotated ? "annotated" : "plain") + " rows=" + read.count());
      System.out.println(read.schema().apply("name").dataType().simpleString());
    }
    spark.stop();
  }
}
