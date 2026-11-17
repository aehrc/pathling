import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/** Prints the optimised plan the engine builds for each case. Usage: EnginePlan dataDir. */
public class EnginePlan {
  static final String[] PATHS = {
    "birthDate",
    "birthDate | deceasedDateTime",
    "birthDate.combine(deceasedDateTime)",
    "(birthDate | deceasedDateTime).first()",
    "iif(active, birthDate, deceasedDateTime)",
    "birthDate.where($this > @1990)",
    "(birthDate | deceasedDateTime).where($this > @1990).exists()",
    "where(birthDate > @1990).birthDate"
  };

  public static void main(final String[] a) {
    final SparkSession spark =
        SparkSession.builder()
            .master("local[2]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.maxMetadataStringLength", "100000")
            .config("spark.sql.adaptive.enabled", "false")
            .getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");
    final PathlingContext ptc = PathlingContext.create(spark);
    final QueryableDataSource data = ptc.read().parquet(a[0]);
    for (final String path : PATHS) {
      final String view =
          "{\"resourceType\":\"ViewDefinition\",\"resource\":\"Patient\",\"status\":\"active\","
              + "\"select\":[{\"column\":[{\"path\":\""
              + path
              + "\",\"name\":\"r\",\"collection\":true}]}]}";
      try {
        final Dataset<Row> df = data.view("Patient").json(view).execute();
        System.out.println("\n=== " + path);
        System.out.println(df.queryExecution().optimizedPlan().toString());
        System.out.println(df.queryExecution().executedPlan().toString());
      } catch (final Exception e) {
        System.out.println("\n=== " + path + "\n  FAILED: " + e.getMessage());
      }
    }
    spark.stop();
  }
}
