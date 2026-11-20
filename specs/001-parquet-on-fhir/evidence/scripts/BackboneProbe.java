import au.csiro.pathling.definition.fhir.FhirDefinitionContext;
import au.csiro.pathling.io.transform.ResourceTransformer;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import ca.uhn.fhir.context.FhirContext;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/** Asks whether a backbone element holding only a modifierExtension, extension or id survives. */
public class BackboneProbe {
  static final String EXT = "{\"url\":\"http://example.com/note\",\"valueString\":\"n\"}";
  static final String NAMED = "{\"name\":{\"family\":\"K\"}}";

  public static void main(String[] args) {
    // Resource type, label, column, document, expressions.
    List<String[]> cases = List.of(
        new String[] {"Patient", "contactModifierExtensionOnly", "contact",
            "{\"resourceType\":\"Patient\",\"id\":\"p\",\"contact\":[" + NAMED + ",{\"modifierExtension\":[" + EXT + "]}]}"},
        new String[] {"Patient", "contactExtensionOnly", "contact",
            "{\"resourceType\":\"Patient\",\"id\":\"p\",\"contact\":[" + NAMED + ",{\"extension\":[" + EXT + "]}]}"},
        new String[] {"Patient", "contactIdOnly", "contact",
            "{\"resourceType\":\"Patient\",\"id\":\"p\",\"contact\":[" + NAMED + ",{\"id\":\"c2\"}]}"},
        new String[] {"Encounter", "hospitalizationModifierExtensionOnly", "hospitalization",
            "{\"resourceType\":\"Encounter\",\"id\":\"e\",\"status\":\"finished\",\"class\":{\"code\":\"IMP\"},\"hospitalization\":{\"modifierExtension\":[" + EXT + "]}}"});

    SparkSession spark = SparkSession.builder().master("local[2]").config("spark.ui.enabled", "false").getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");
    PathlingContext ptc = PathlingContext.create(spark);
    ResourceTransformer tx = ResourceTransformer.of(FhirDefinitionContext.of(FhirContext.forR4()));

    for (String[] c : cases) {
      String type = c[0], col = c[2], doc = c[3];
      System.out.println("\n##### " + c[1]);
      System.out.println("input:       " + doc);
      Dataset<String> ds = spark.createDataset(List.of(doc), Encoders.STRING());
      Dataset<Row> old = ptc.encode(ds.toDF("value"), type).cache();
      Dataset<Row> neu = tx.toLayout(type, spark.read().option("mode", "FAILFAST").json(ds)).cache();
      for (var l : List.of(java.util.Map.entry("old", old), java.util.Map.entry("new", neu))) {
        String v;
        try {
          v = String.valueOf(l.getValue().selectExpr("to_json(" + col + ")").first().get(0));
        } catch (Exception ex) {
          v = "(no column)";
        }
        System.out.println(l.getKey() + " stored:  " + v);
      }
      System.out.println("new written: " + tx.toJsonShape(type, neu).toJSON().first());
      List<String> paths = List.of(col + ".count()", col + ".exists()", col + ".modifierExtension.count()",
          col + ".id");
      for (var layout : List.of(java.util.Map.entry("old", old), java.util.Map.entry("new", neu))) {
        QueryableDataSource data = ptc.read().datasets().dataset(type, layout.getValue());
        for (String p : paths) {
          String r;
          try {
            String view = "{\"resourceType\":\"ViewDefinition\",\"resource\":\"" + type + "\",\"status\":\"active\",\"select\":[{\"column\":[{\"path\":\""
                + p + "\",\"name\":\"r\",\"collection\":true}]}]}";
            r = String.valueOf(data.view(type).json(view).execute().first().get(0));
          } catch (Exception ex) {
            r = "FAILED " + ex.getClass().getSimpleName() + ": " + String.valueOf(ex.getMessage()).lines().findFirst().orElse("");
          }
          System.out.printf("  %-3s %-40s -> %s%n", layout.getKey(), p, r);
        }
      }
    }
    spark.stop();
  }
}
