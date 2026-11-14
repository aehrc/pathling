import au.csiro.pathling.definition.fhir.FhirDefinitionContext;
import au.csiro.pathling.io.transform.ResourceTransformer;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import ca.uhn.fhir.context.FhirContext;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/** Asks whether a complex element holding only an id, extension or modifierExtension survives. */
public class MetadataSlotProbe {
  static final String DAR = "{\"url\":\"http://hl7.org/fhir/StructureDefinition/data-absent-reason\",\"valueCode\":\"masked\"}";
  static final String EXT = "{\"url\":\"http://example.com/note\",\"valueString\":\"n\"}";

  static String names(String middle) {
    return "{\"resourceType\":\"Patient\",\"id\":\"p\",\"name\":[{\"family\":\"x\"}," + middle + ",{\"family\":\"y\"}]}";
  }

  public static void main(String[] args) {
    Map<String, String> docs = new LinkedHashMap<>();
    docs.put("idOnly", names("{\"id\":\"n2\"}"));
    docs.put("modifierExtensionOnly", names("{\"modifierExtension\":[" + EXT + "]}"));
    docs.put("extensionOnly", names("{\"extension\":[" + EXT + "]}"));
    docs.put("idPlusDarFamily", names("{\"id\":\"n2\",\"_family\":{\"extension\":[" + DAR + "]}}"));
    docs.put("singularModifierExtension", "{\"resourceType\":\"Patient\",\"id\":\"p\",\"maritalStatus\":{\"modifierExtension\":[" + EXT + "]}}");

    SparkSession spark = SparkSession.builder().master("local[2]").config("spark.ui.enabled", "false").getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");
    PathlingContext ptc = PathlingContext.create(spark);
    ResourceTransformer tx = ResourceTransformer.of(FhirDefinitionContext.of(FhirContext.forR4()));

    List<String> paths = List.of("name.count()", "name.id", "name.extension.count()",
        "name.modifierExtension.count()", "name.where(family.empty()).count()",
        "maritalStatus.exists()", "maritalStatus.modifierExtension.count()");

    for (var e : docs.entrySet()) {
      System.out.println("\n##### " + e.getKey());
      System.out.println("input:       " + e.getValue());
      Dataset<String> ds = spark.createDataset(List.of(e.getValue()), Encoders.STRING());
      Dataset<Row> old = ptc.encode(ds.toDF("value"), "Patient").cache();
      Dataset<Row> neu = tx.toLayout("Patient", spark.read().option("mode", "FAILFAST").json(ds)).cache();
      for (String col : List.of("name", "maritalStatus")) {
        for (var l : List.of(Map.entry("old", old), Map.entry("new", neu))) {
          String v;
          try {
            v = String.valueOf(l.getValue().selectExpr("to_json(" + col + ")").first().get(0));
          } catch (Exception ex) {
            v = "(no column)";
          }
          System.out.printf("%s %-13s %s%n", l.getKey(), col + ":", v);
        }
      }
      System.out.println("new written: " + tx.toJsonShape("Patient", neu).toJSON().first());
      for (var layout : List.of(Map.entry("old", old), Map.entry("new", neu))) {
        QueryableDataSource data = ptc.read().datasets().dataset("Patient", layout.getValue());
        for (String p : paths) {
          String r;
          try {
            String view = "{\"resourceType\":\"ViewDefinition\",\"resource\":\"Patient\",\"status\":\"active\",\"select\":[{\"column\":[{\"path\":\""
                + p + "\",\"name\":\"r\",\"collection\":true}]}]}";
            r = String.valueOf(data.view("Patient").json(view).execute().first().get(0));
          } catch (Exception ex) {
            r = "FAILED " + ex.getClass().getSimpleName() + ": " + String.valueOf(ex.getMessage()).lines().findFirst().orElse("");
          }
          System.out.printf("  %-3s %-42s -> %s%n", layout.getKey(), p, r);
        }
      }
    }
    spark.stop();
  }
}
