import au.csiro.pathling.definition.fhir.FhirDefinitionContext;
import au.csiro.pathling.io.transform.ResourceTransformer;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import ca.uhn.fhir.context.FhirContext;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class LayoutProbe {
  static final String DAR = "{\"url\":\"http://hl7.org/fhir/StructureDefinition/data-absent-reason\",\"valueCode\":\"masked\"}";

  public static void main(String[] args) throws Exception {
    Map<String, String> docs = new LinkedHashMap<>();
    docs.put("pe2", Files.readString(Path.of(args[0])).replaceAll("\\s*\\n\\s*", ""));
    docs.put("darFamily", "{\"resourceType\":\"Patient\",\"id\":\"b\",\"name\":[{\"family\":\"x\"},{\"_family\":{\"extension\":[" + DAR + "]}},{\"family\":\"y\"}]}");
    docs.put("trailingGiven", "{\"resourceType\":\"Patient\",\"id\":\"c\",\"name\":[{\"given\":[\"Ann\"],\"_given\":[null,{\"extension\":[" + DAR + "]}]}]}");
    docs.put("nullGiven", "{\"resourceType\":\"Patient\",\"id\":\"d\",\"name\":[{\"given\":[\"Ann\",null,\"Bee\"],\"_given\":[null,{\"extension\":[" + DAR + "]},null]}]}");

    SparkSession spark = SparkSession.builder().master("local[2]").config("spark.ui.enabled", "false").getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");
    PathlingContext ptc = PathlingContext.create(spark);
    ResourceTransformer tx = ResourceTransformer.of(FhirDefinitionContext.of(FhirContext.forR4()));

    List<String> paths = List.of("name.given", "name.given.count()", "name.given[3]", "name.given.exists()",
        "name.given.distinct().count()", "name.family", "name.family.count()", "name.count()",
        "name.where(family.exists()).count()", "name.where(family.empty()).count()", "name.exists()");

    for (var e : docs.entrySet()) {
      System.out.println("\n##### " + e.getKey());
      Dataset<String> ds = spark.createDataset(List.of(e.getValue()), Encoders.STRING());
      Dataset<Row> old = ptc.encode(ds.toDF("value"), "Patient").cache();
      Dataset<Row> neu = tx.toLayout("Patient", spark.read().option("mode", "FAILFAST").json(ds)).cache();
      System.out.println("old stored: " + old.selectExpr("to_json(name)").first().get(0));
      System.out.println("new stored: " + neu.selectExpr("to_json(name)").first().get(0));
      for (var layout : List.of(Map.entry("old", old), Map.entry("new", neu))) {
        QueryableDataSource data = ptc.read().datasets().dataset("Patient", layout.getValue());
        for (String p : paths) {
          String r;
          try {
            String view = "{\"resourceType\":\"ViewDefinition\",\"resource\":\"Patient\",\"status\":\"active\",\"select\":[{\"column\":[{\"path\":\""
                + p.replace("'", "\\u0027") + "\",\"name\":\"r\",\"collection\":true}]}]}";
            r = String.valueOf(data.view("Patient").json(view).execute().first().get(0));
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
