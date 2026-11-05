import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class OnlyNull {

  // A name whose only given is value-less: the array holds one JSON null.
  private static final String PATIENT =
      "{\"resourceType\":\"Patient\",\"id\":\"3\",\"name\":[{\"given\":[null],"
          + "\"_given\":[{\"extension\":[{\"url\":\"http://example.com/dar\","
          + "\"valueCode\":\"masked\"}]}]}]}";

  public static void main(final String[] args) {
    final SparkSession spark =
        SparkSession.builder().appName("probe").master("local[2]").getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");
    final PathlingContext ptc = PathlingContext.create(spark);
    final Dataset<Row> json =
        spark.createDataset(List.of(PATIENT), Encoders.STRING()).toDF("value");
    final Dataset<Row> encoded = ptc.encode(json, "Patient").cache();
    System.out.println(
        "stored = " + encoded.selectExpr("name[0].given as g").collectAsList().get(0).getList(0));
    final QueryableDataSource data = ptc.read().datasets().dataset("Patient", encoded);
    for (final String path :
        List.of(
            "name.given",
            "name.given.count()",
            "name.given.empty()",
            "name.given.exists()",
            "name.given.first()",
            "name.exists(given = 'Ann')",
            "name.given.hasValue()",
            "name.given.extension.count()")) {
      final String view =
          "{\"resourceType\":\"ViewDefinition\",\"resource\":\"Patient\",\"status\":\"active\","
              + "\"select\":[{\"column\":[{\"path\":\"id\",\"name\":\"id\"},{\"path\":\""
              + path.replace("'", "\\u0027")
              + "\",\"name\":\"result\",\"collection\":true}]}]}";
      try {
        System.out.println(
            "  " + path + " -> " + data.view("Patient").json(view).execute().collectAsList());
      } catch (final Exception e) {
        System.out.println("  " + path + " -> FAILED " + e.getClass().getSimpleName());
      }
    }
    spark.stop();
  }
}
