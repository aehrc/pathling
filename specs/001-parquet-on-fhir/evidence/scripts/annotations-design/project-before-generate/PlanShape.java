import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.PlanRewrite;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/** Prints the analysed, rewritten and optimised plans of one view. Usage: PlanShape dataDir. */
public class PlanShape {
  public static void main(final String[] args) throws Exception {
    final SparkSession spark = SparkSession.builder().master("local[2]")
        .config("spark.ui.enabled", "false").config("spark.sql.adaptive.enabled", "false")
        .getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");
    final PathlingContext ptc = PathlingContext.create(spark);
    final QueryableDataSource names = ptc.read().parquet(args[0] + "/engine");
    final String v = args.length > 1 ? args[1]
        : "{\"resourceType\":\"ViewDefinition\",\"resource\":\"Patient\",\"status\":\"active\","
        + "\"select\":[{\"column\":[{\"path\":\"family\",\"name\":\"r\"}],\"forEach\":\"name\"}]}";
    final Dataset<Row> df = names.view("Patient").json(v).execute();
    System.out.println("ANALYSED\n" + PlanRewrite.analysedPlan(df));
    System.out.println("REWRITTEN\n" + PlanRewrite.rewrittenPlan(df));
    final Dataset<Row> rw = PlanRewrite.rewrite(df);
    System.out.println("OPTIMISED\n" + PlanRewrite.optimised(rw));
    spark.stop();
  }
}
