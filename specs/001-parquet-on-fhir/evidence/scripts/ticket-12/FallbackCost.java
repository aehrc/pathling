/**
 * Ticket 12, T096c: the cost of the quantity inequality's fallback from whole-stage code
 * generation (decisions 103 and 126). Each expression is evaluated as a filter over the cached
 * Observations, and the best of the timed counts is printed with whether the filter is part of
 * whole-stage code generation.
 *
 * <p>The equality and the inequality are compared with an ordering, which generates code, alone and
 * beside a Coding filter that already falls back for its higher-order filter. The negated equality
 * is included because it was the candidate rewrite of the inequality.
 *
 * <p>Usage: FallbackCost <directory holding Observation> [runs].
 */
import au.csiro.pathling.library.PathlingContext;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.WholeStageCodegenExec;
import scala.jdk.javaapi.CollectionConverters;

public class FallbackCost {

  public static void main(final String[] args) {
    final SparkSession spark =
        SparkSession.builder().master("local[4]").config("spark.ui.enabled", "false").getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");
    final PathlingContext pc = PathlingContext.create(spark);
    final Dataset<Row> obs = spark.read().parquet(args[0] + "/Observation").cache();
    final long total = obs.count();
    final int runs = args.length > 1 ? Integer.parseInt(args[1]) : 7;
    final String q = "value.ofType(Quantity)";
    final String coding =
        "code.coding.where(system = 'http://loinc.org' and code = '2093-3').exists()";
    final Map<String, String> exprs = new LinkedHashMap<>();
    exprs.put("q.value > 5.5", q + ".value > 5.5");
    exprs.put("q > 50", q + " > 50 'mg/dL'");
    exprs.put("q = 50", q + " = 50 'mg/dL'");
    exprs.put("q != 50", q + " != 50 'mg/dL'");
    exprs.put("(q = 50).not()", "(" + q + " = 50 'mg/dL').not()");
    exprs.put("coding", coding);
    exprs.put("coding and q > 50", coding + " and " + q + " > 50 'mg/dL'");
    exprs.put("coding and q = 50", coding + " and " + q + " = 50 'mg/dL'");
    exprs.put("coding and q != 50", coding + " and " + q + " != 50 'mg/dL'");
    exprs.put("coding and (q = 50).not()", coding + " and (" + q + " = 50 'mg/dL').not()");
    System.out.println("ROWS " + total);
    for (final Map.Entry<String, String> e : exprs.entrySet()) {
      final Dataset<Row> query =
          obs.where(pc.fhirPathToColumn("Observation", e.getValue()).alias("x"));
      final SparkPlan plan = query.queryExecution().executedPlan();
      long best = Long.MAX_VALUE;
      long sum = 0;
      long rows = 0;
      for (int i = 0; i < runs; i++) {
        final long start = System.nanoTime();
        rows = query.count();
        final long elapsed = (System.nanoTime() - start) / 1_000_000;
        // The first run is a warm-up.
        if (i > 0) {
          best = Math.min(best, elapsed);
          sum += elapsed;
        }
      }
      System.out.println(
          "COST wholeStage="
              + filterIsWholeStage(plan)
              + " best="
              + best
              + "ms mean="
              + (sum / (runs - 1))
              + "ms rows="
              + rows
              + " :: "
              + e.getKey());
    }
    spark.stop();
  }

  /** Whether the filter is evaluated inside a whole-stage code generation stage. */
  private static boolean filterIsWholeStage(final SparkPlan plan) {
    for (final SparkPlan node : CollectionConverters.asJava(plan.collect(
        new scala.PartialFunction<SparkPlan, SparkPlan>() {
          @Override
          public boolean isDefinedAt(final SparkPlan p) {
            return p instanceof WholeStageCodegenExec;
          }

          @Override
          public SparkPlan apply(final SparkPlan p) {
            return p;
          }
        }))) {
      if (node.toString().contains("Filter")) {
        return true;
      }
    }
    return false;
  }
}
