import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructType;
import spike.CaseProbe;
import spike.LambdaPruning;

/**
 * Case 4, extended: the eight A-raw models of the singular-meta-pruning spike (element
 * struct(value, meta = raw _x), union with a dedupe aggregate, where, comparison lambdas, all in
 * the view's inline wrapper), with the rule off and on. Reuses SingularMetaProbe.model, compiled
 * onto the classpath by build.sh. Usage: ARawProbe singularParquetDir [-v].
 */
public class ARawProbe {

  public static void main(final String[] args) {
    final SparkSession spark =
        SparkSession.builder()
            .master("local[2]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.adaptive.enabled", "false")
            .getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");
    LambdaPruning.verbose_$eq(Arrays.asList(args).contains("-v"));
    final Dataset<Row> raw = spark.read().parquet(args[0] + "/Patient.parquet");
    final StructType full = raw.schema();
    System.out.println("| A-raw model | Reads without the rule | Reads with it | Results equal |");
    System.out.println("| --- | --- | --- | --- |");
    for (int c = 0; c < SingularMetaProbe.CASE_PATHS.length; c++) {
      final String expr = "array(struct(" + SingularMetaProbe.model("A-raw", c) + " AS r))";
      LambdaPruning.uninstall(spark);
      final Dataset<Row> off = raw.select(functions.inline(functions.expr(expr)));
      final String offRead = CaseProbe.compact(CaseProbe.readSchema(off), full);
      final List<String> offRows = off.collectAsList().stream().map(Row::toString).sorted().toList();
      LambdaPruning.install(spark);
      final Dataset<Row> on = raw.select(functions.inline(functions.expr(expr)));
      final String onRead = CaseProbe.compact(CaseProbe.readSchema(on), full);
      final List<String> onRows = on.collectAsList().stream().map(Row::toString).sorted().toList();
      final boolean same = offRows.equals(onRows) && off.schema().equals(on.schema());
      System.out.printf("| `%s` | %s | %s | %s (%d rows) |%n", SingularMetaProbe.CASE_PATHS[c],
          offRead, onRead, same ? "yes" : "**NO**", onRows.size());
      LambdaPruning.uninstall(spark);
    }
    spark.stop();
  }
}
