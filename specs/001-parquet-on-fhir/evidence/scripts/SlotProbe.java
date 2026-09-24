import au.csiro.pathling.definition.fhir.FhirDefinitionContext;
import au.csiro.pathling.io.transform.ResourceTransformer;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import ca.uhn.fhir.context.FhirContext;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class SlotProbe {
  public static void main(String[] args) {
    String doc = "{\"resourceType\":\"Patient\",\"id\":\"b\",\"name\":[{\"family\":\"x\"},{\"_family\":{\"extension\":[{\"url\":\"http://hl7.org/fhir/StructureDefinition/data-absent-reason\",\"valueCode\":\"masked\"}]}},{\"family\":\"y\"}],"
        + "\"maritalStatus\":{\"_text\":{\"extension\":[{\"url\":\"u\",\"valueCode\":\"masked\"}]}}}";
    SparkSession spark = SparkSession.builder().master("local[2]").config("spark.ui.enabled", "false").getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");
    PathlingContext ptc = PathlingContext.create(spark);
    ResourceTransformer tx = ResourceTransformer.of(FhirDefinitionContext.of(FhirContext.forR4()));
    Dataset<String> ds = spark.createDataset(List.of(doc), Encoders.STRING());
    Dataset<Row> old = ptc.encode(ds.toDF("value"), "Patient").cache();
    Dataset<Row> empty = tx.toLayout("Patient", spark.read().json(ds)).cache();
    // Null the all-null struct, keeping its slot.
    Dataset<Row> slot = empty
        .withColumn("name", transform(col("name"), n -> when(n.getField("family").isNotNull(), n)))
                .cache();
    // Drop the slot as well.
    Dataset<Row> dropped = slot.withColumn("name", filter(col("name"), n -> n.isNotNull())).cache();
    List<String> paths = List.of("name.count()", "name.family.count()", "name.where(family.exists()).count()",
        "name.where(family.empty()).count()", "name[1].exists()", "name[2].family", "name.exists()",
        "name.select(family).count()", "name.first().family", "name.last().family");
    for (var layout : List.of(Map.entry("old", old), Map.entry("empty{}", empty), Map.entry("null-slot", slot), Map.entry("dropped", dropped))) {
      System.out.println("## " + layout.getKey() + "  name=" + layout.getValue().selectExpr("to_json(name)").first().get(0)
         );
      QueryableDataSource data = ptc.read().datasets().dataset("Patient", layout.getValue());
      for (String p : paths) {
        String r;
        try {
          String view = "{\"resourceType\":\"ViewDefinition\",\"resource\":\"Patient\",\"status\":\"active\",\"select\":[{\"column\":[{\"path\":\""
              + p + "\",\"name\":\"r\",\"collection\":true}]}]}";
          r = String.valueOf(data.view("Patient").json(view).execute().first().get(0));
        } catch (Exception ex) {
          r = "FAILED " + String.valueOf(ex.getMessage()).lines().findFirst().orElse("");
        }
        System.out.printf("  %-40s -> %s%n", p, r);
      }
      if (!layout.getKey().equals("old")) {
        System.out.println("  spark json out: " + layout.getValue().select(to_json(struct(col("name")))).first().get(0));
      }
    }
    spark.stop();
  }
}
