/**
 * Ticket 12: T100o's plan-size measurement repeated over the Parquet on FHIR layout, with and
 * without the canonical annotations. Prints, for each expression, the length of the optimised
 * plan's expressions, its CASE WHEN count, its calls of the canonicalisation (quantity_canonical),
 * the occurrences of __canonical_exact, and the best of six timed counts.
 *
 * <p>Usage: PlanSize <directory holding Observation>.
 */
import au.csiro.pathling.library.PathlingContext;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/**
 * Usage: PlanSize <parquetDir>. Prints, for each expression, the length of the optimised plan's
 * expressions, the number of CASE WHEN in them, the number of calls to the canonical_quantity_value
 * UDF in them (one per decode of a stored quantity, T100o) and the best of six timed counts.
 *
 * <p>The first seven expressions are the M2 review's; the rest were added by T100o to measure a
 * stored quantity decoded once per operation that computes with it (decision 82).
 */
public class PlanSize {
  public static void main(String[] args) {
    final SparkSession spark = SparkSession.builder().master("local[4]")
        .config("spark.ui.enabled", "false").getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");
    final PathlingContext pc = PathlingContext.create(spark);
    final Dataset<Row> obs = spark.read().parquet(args[0] + "/Observation").cache();
    obs.count();
    final String q = "value.ofType(Quantity)";
    final String c = "component.value.ofType(Quantity)";
    final List<String> exprs = List.of(
        // The M2 review's expressions.
        q + ".value",
        q,
        q + " > 100 'mg/dL'",
        q + ".value > 5.5",
        q + ".unit = 'cm'",
        c + ".value",
        "referenceRange.low.value",
        // T100o: one stored quantity in several comparisons.
        q + " > 1 'mg/dL' and " + q + " < 1000 'mg/dL'",
        q + " > 1 'mg/dL' and " + q + " < 1000 'mg/dL' and " + q + " != 50 'mg/dL'",
        // T100o: a where over a repeating quantity, with one and with two comparisons.
        c + ".where($this > 100 'mm[Hg]')",
        c + ".where($this > 100 'mm[Hg]' and $this < 200 'mm[Hg]')",
        // T100o: toString(), and a union with a literal.
        q + ".toString()",
        q + " | 7 'g'");
    for (final String e : exprs) {
      final Dataset<Row> query = obs.select(pc.fhirPathToColumn("Observation", e).alias("x"));
      final LogicalPlan plan = query.queryExecution().optimizedPlan();
      final String text = plan.expressions().mkString(",");
      final int caseWhen = text.split("CASE WHEN", -1).length - 1;
      final int decodes = text.split("quantity_canonical\\(", -1).length - 1;
      final int reads = text.split("__canonical_exact", -1).length - 1;
      long best = Long.MAX_VALUE;
      long rows = 0;
      for (int i = 0; i < 6; i++) {
        final long start = System.nanoTime();
        rows = query.where("x is not null and cast(x as string) <> 'false'").count();
        best = Math.min(best, (System.nanoTime() - start) / 1_000_000);
      }
      System.out.println("PLAN chars=" + text.length() + " casewhen=" + caseWhen + " udf="
          + decodes + " exactReads=" + reads + " best=" + best + "ms rows=" + rows + " :: " + e);
    }
    spark.stop();
  }
}
