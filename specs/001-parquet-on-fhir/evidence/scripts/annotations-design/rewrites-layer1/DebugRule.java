import au.csiro.pathling.library.PathlingContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.LayerRewrites;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import spike.CaseProbe;
import spike.LambdaPruning;

/** Runs one view as P+R+D3b with the rule verbose. Usage: DebugRule dataDir viewJson. */
public class DebugRule {
  public static void main(final String[] args) {
    final SparkSession spark = SparkSession.builder().master("local[2]")
        .config("spark.ui.enabled", "false").config("spark.sql.adaptive.enabled", "false")
        .getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");
    final PathlingContext ptc = PathlingContext.create(spark);
    LambdaPruning.verbose_$eq(true);
    LambdaPruning.install(spark);
    final Dataset<Row> df = LayerRewrites.run(
        ptc.read().parquet(args[0] + "/engine").view("Patient").json(args[1]).execute(),
        true, true, true);
    System.out.println("READS " + CaseProbe.readSchema(df).catalogString());
    System.out.println(df.queryExecution().optimizedPlan().treeString());
    spark.stop();
  }
}
