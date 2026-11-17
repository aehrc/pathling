import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import java.io.PrintWriter;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Runs the views of the rewrites-layer1 spike through the real engine, with the original plan only
 * (no rewrite rules installed), over one shared set of Parquet on FHIR tables, and writes each
 * view's output schema and full sorted rows to a file, so that the files of two Spark versions can
 * be compared. With the argument "inputs", it instead compares two directories of encoded tables
 * (a schema comparison and a row-level except in both directions), to show whether the encoders
 * of the two Spark versions produced identical input. Usage: ResultsDump out.txt engineDir edgeDir
 * singularDir, or ResultsDump inputs dirA dirB.
 */
public class ResultsDump {

  static SparkSession session() {
    final SparkSession spark =
        SparkSession.builder()
            .master("local[2]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.adaptive.enabled", "false")
            .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
            .getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");
    return spark;
  }

  static void dump(
      final PrintWriter out,
      final QueryableDataSource data,
      final String prefix,
      final List<String[]> views) {
    for (final String[] v : views) {
      final String label = prefix + v[0];
      out.println("VIEW\t" + label.replace("\t", " "));
      try {
        final Dataset<Row> df = data.view("Patient").json(v[1]).execute();
        out.println("SCHEMA\t" + df.schema().catalogString());
        final List<String> rows = df.collectAsList().stream().map(Row::toString).sorted().toList();
        out.println("COUNT\t" + rows.size());
        rows.forEach(r -> out.println("ROW\t" + r.replace("\n", "\\n")));
      } catch (final Exception e) {
        out.println("ERROR\t" + String.valueOf(e).replace("\n", " "));
      }
      out.println("END");
    }
  }

  static void compareInputs(final SparkSession spark, final String a, final String b) {
    for (final String rel : new String[] {"engine/Patient.*.parquet", "edge/Patient.parquet"}) {
      final Dataset<Row> da = spark.read().parquet(a + "/" + rel);
      final Dataset<Row> db = spark.read().parquet(b + "/" + rel);
      final boolean sameSchema = da.schema().equals(db.schema());
      final long ca = da.count();
      final long cb = db.count();
      final long ab = da.except(db).count();
      final long ba = db.except(da).count();
      System.out.println(
          "INPUT\t" + rel + "\tschemaEqual=" + sameSchema + "\trowsA=" + ca + "\trowsB=" + cb
              + "\texceptAB=" + ab + "\texceptBA=" + ba);
    }
  }

  public static void main(final String[] args) throws Exception {
    final SparkSession spark = session();
    System.out.println("Spark " + spark.version());
    if (args[0].equals("inputs")) {
      compareInputs(spark, args[1], args[2]);
      spark.stop();
      return;
    }
    final PathlingContext ptc = PathlingContext.create(spark);
    final List<String[]> names = ProbeMain.nameViews();
    final List<String[]> singular = ProbeMain.singularViews();
    try (PrintWriter out = new PrintWriter(args[0], "UTF-8")) {
      out.println("SPARK\t" + spark.version());
      dump(out, ptc.read().parquet(args[1]), "", names);
      dump(out, ptc.read().parquet(args[3]), "", singular);
      dump(out, ptc.read().parquet(args[2]), "edge: ", names);
    }
    spark.stop();
  }
}
