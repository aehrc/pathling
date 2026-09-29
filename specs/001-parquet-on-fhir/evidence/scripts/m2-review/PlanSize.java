import au.csiro.pathling.library.PathlingContext;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/** Usage: PlanSize <parquetDir>. Prints optimised expression size, CASE WHEN count and best time. */
public class PlanSize {
  public static void main(String[] args) {
    final SparkSession spark = SparkSession.builder().master("local[4]")
        .config("spark.ui.enabled", "false").getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");
    final PathlingContext pc = PathlingContext.create(spark);
    final Dataset<Row> obs = spark.read().parquet(args[0] + "/Observation").cache();
    obs.count();
    final List<String> exprs = List.of(
        "value.ofType(Quantity).value",
        "value.ofType(Quantity)",
        "value.ofType(Quantity) > 100 'mg/dL'",
        "value.ofType(Quantity).value > 5.5",
        "value.ofType(Quantity).unit = 'cm'",
        "component.value.ofType(Quantity).value",
        "referenceRange.low.value");
    for (final String e : exprs) {
      final Dataset<Row> q = obs.select(pc.fhirPathToColumn("Observation", e).alias("x"));
      final LogicalPlan plan = q.queryExecution().optimizedPlan();
      final String text = plan.expressions().mkString(",");
      final int caseWhen = text.split("CASE WHEN", -1).length - 1;
      long best = Long.MAX_VALUE;
      long rows = 0;
      for (int i = 0; i < 6; i++) {
        final long start = System.nanoTime();
        rows = q.where("x is not null and cast(x as string) <> 'false'").count();
        best = Math.min(best, (System.nanoTime() - start) / 1_000_000);
      }
      System.out.println("PLAN chars=" + text.length() + " casewhen=" + caseWhen + " best=" + best
          + "ms rows=" + rows + " :: " + e);
    }
    spark.stop();
  }
}
