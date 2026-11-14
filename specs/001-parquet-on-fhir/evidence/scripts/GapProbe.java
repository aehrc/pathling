import au.csiro.pathling.definition.fhir.FhirDefinitionContext;
import au.csiro.pathling.io.transform.ResourceTransformer;
import ca.uhn.fhir.context.FhirContext;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Asks what the writer emits for positional nulls whose _x sibling is not stored, and whether a
 * conformant document is emptied by a neighbour that re-types a column (FR-016 item 6).
 */
public class GapProbe {
  static final String DAR = "{\"url\":\"http://hl7.org/fhir/StructureDefinition/data-absent-reason\",\"valueCode\":\"masked\"}";

  public static void main(String[] args) {
    SparkSession spark = SparkSession.builder().master("local[2]").config("spark.ui.enabled", "false").getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");
    ResourceTransformer tx = ResourceTransformer.of(FhirDefinitionContext.of(FhirContext.forR4()));

    run(spark, tx, "positionalNull", List.of(
        "{\"resourceType\":\"Patient\",\"id\":\"a\",\"name\":[{\"given\":[\"Ann\",null,\"Bee\"],\"_given\":[null,{\"extension\":[" + DAR + "]},null]}]}"));
    run(spark, tx, "onlyNull", List.of(
        "{\"resourceType\":\"Patient\",\"id\":\"a\",\"name\":[{\"family\":\"F\",\"given\":[null],\"_given\":[{\"extension\":[" + DAR + "]}]}]}"));
    run(spark, tx, "neighbourRetypes", List.of(
        "{\"resourceType\":\"Patient\",\"id\":\"a\",\"photo\":[{\"contentType\":\"image/png\"},{\"size\":10}]}",
        "{\"resourceType\":\"Patient\",\"id\":\"b\",\"photo\":[{\"size\":1.5}]}"));
    spark.stop();
  }

  static void run(SparkSession spark, ResourceTransformer tx, String label, List<String> docs) {
    System.out.println("\n##### " + label);
    docs.forEach(d -> System.out.println("input:   " + d));
    Dataset<String> ds = spark.createDataset(docs, Encoders.STRING());
    Dataset<Row> stored = tx.toLayout("Patient", spark.read().option("mode", "FAILFAST").json(ds));
    tx.toJsonShape("Patient", stored).toJSON().collectAsList()
        .forEach(w -> System.out.println("written: " + w));
  }
}
