/**
 * Prints the schema that PathlingContext.encode produces for Patient and Encounter NDJSON, which is
 * what the splice in Splice.java extends. Usage: SchemaProbe <ndjson dir>.
 */
import au.csiro.pathling.library.PathlingContext;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SchemaProbe {

  public static void main(final String[] args) throws Exception {
    final SparkSession spark =
        SparkSession.builder().master("local[2]").config("spark.ui.enabled", "false").getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");
    final PathlingContext ptc = PathlingContext.create(spark);
    for (final String t : new String[] {"Patient", "Encounter"}) {
      final List<String> lines = Files.readAllLines(Path.of(args[0], t + ".ndjson"));
      final Dataset<Row> df =
          ptc.encode(spark.createDataset(lines, Encoders.STRING()).toDF(), t);
      System.out.println("SCHEMA " + t + " " + df.schema().treeString());
    }
    spark.stop();
  }
}
