import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Asks which stored columns Parquet reads for a FHIRPath column of a view, run through the engine
 * on data stored in the layout. Takes the directory of NDJSON files to store.
 */
public class EnginePruningProbe {

  static String readSchema(final Dataset<Row> df) {
    final String plan = df.queryExecution().executedPlan().toString();
    final Matcher m = Pattern.compile("ReadSchema: (struct<.*?>)\\s*$", Pattern.MULTILINE).matcher(plan);
    String last = "<not found>";
    while (m.find()) {
      last = m.group(1);
    }
    return last;
  }

  static String view(final String resource, final String forEach, final String path) {
    final String column = "{\"column\":[{\"path\":\"" + path + "\",\"name\":\"r\",\"collection\":true}]";
    final String select = forEach == null ? column + "}" : column + ",\"forEach\":\"" + forEach + "\"}";
    return "{\"resourceType\":\"ViewDefinition\",\"resource\":\"" + resource
        + "\",\"status\":\"active\",\"select\":[" + select + "]}";
  }

  public static void main(final String[] args) throws Exception {
    final SparkSession spark =
        SparkSession.builder()
            .master("local[2]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.maxMetadataStringLength", "100000")
            .config("spark.sql.adaptive.enabled", "false")
            .getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");
    final PathlingContext ptc = PathlingContext.create(spark);
    final Path out = Files.createTempDirectory("engine-pruning");
    ptc.read().ndjson(args[0]).write().parquet(out.toString());
    final QueryableDataSource data = ptc.read().parquet(out.toString());

    final List<Map.Entry<String, String[]>> cases =
        List.of(
            Map.entry("Patient", new String[] {null, "birthDate"}),
            Map.entry("Patient", new String[] {null, "name.family"}),
            Map.entry("Patient", new String[] {null, "name.given"}),
            Map.entry("Patient", new String[] {null, "name.period.start"}),
            Map.entry("Patient", new String[] {null, "address.period.start"}),
            Map.entry("Patient", new String[] {"name", "family"}),
            Map.entry("Patient", new String[] {"name", "period.start"}),
            Map.entry("Observation", new String[] {null, "valueQuantity.value"}),
            Map.entry("Observation", new String[] {null, "code.text"}),
            Map.entry("Observation", new String[] {null, "code.coding.code"}),
            Map.entry("Observation", new String[] {null, "component.code.text"}),
            Map.entry("Observation", new String[] {null, "component.code.coding.code"}),
            Map.entry("Observation", new String[] {null, "component.valueQuantity.value"}),
            Map.entry("Observation", new String[] {"component", "valueQuantity.value"}));

    for (final Map.Entry<String, String[]> c : cases) {
      final String forEach = c.getValue()[0];
      final String path = c.getValue()[1];
      final Dataset<Row> result = data.view(c.getKey()).json(view(c.getKey(), forEach, path)).execute();
      System.out.printf("%n%s %s%s%n  reads: %s%n  value: %s%n", c.getKey(),
          forEach == null ? "" : "forEach " + forEach + " / ", path, readSchema(result),
          result.collectAsList());
    }
    spark.stop();
  }
}
