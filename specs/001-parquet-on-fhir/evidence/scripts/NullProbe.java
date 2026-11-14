import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import ca.uhn.fhir.context.FhirContext;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Patient;

public class NullProbe {

  // A conformant R4 patient: the middle given name has no value, only an extension in _given.
  private static final String PATIENT =
      "{\"resourceType\":\"Patient\",\"id\":\"1\",\"name\":[{\"given\":[\"Ann\",null,\"Bee\"],"
          + "\"_given\":[null,{\"extension\":[{\"url\":\"http://example.com/dar\","
          + "\"valueCode\":\"masked\"}]},null]}]}";

  // The same shape without any _given at all.
  private static final String PATIENT_NO_UNDERSCORE =
      "{\"resourceType\":\"Patient\",\"id\":\"2\",\"name\":[{\"given\":[\"Ann\",null,\"Bee\"]}]}";

  public static void main(final String[] args) {
    // First: what HAPI itself makes of it, with no Spark involved.
    final FhirContext fhirContext = FhirContext.forR4();
    for (final String document : List.of(PATIENT, PATIENT_NO_UNDERSCORE)) {
      try {
        final Patient patient =
            (Patient) fhirContext.newJsonParser().parseResource(document);
        System.out.println("HAPI given size = " + patient.getName().get(0).getGiven().size());
        patient
            .getName()
            .get(0)
            .getGiven()
            .forEach(
                g ->
                    System.out.println(
                        "  value=" + g.getValue() + " extensions=" + g.getExtension().size()));
      } catch (final Exception e) {
        System.out.println("HAPI parse failed: " + e);
      }
    }

    final SparkSession spark =
        SparkSession.builder().appName("probe").master("local[2]").getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");
    final PathlingContext ptc = PathlingContext.create(spark);

    for (final String document : List.of(PATIENT, PATIENT_NO_UNDERSCORE)) {
      System.out.println("=== " + document.substring(0, 60));
      final Dataset<Row> json =
          spark.createDataset(List.of(document), Encoders.STRING()).toDF("value");
      final Dataset<Row> encoded;
      try {
        encoded = ptc.encode(json, "Patient").cache();
        encoded.selectExpr("name[0].given as given").show(false);
        final Row row = encoded.selectExpr("name[0].given as given").collectAsList().get(0);
        System.out.println("stored list = " + row.getList(0));
      } catch (final Exception e) {
        System.out.println("encode failed: " + e);
        continue;
      }

      final QueryableDataSource data = ptc.read().datasets().dataset("Patient", encoded);
      for (final String path :
          List.of(
              "name.given",
              "name.given.count()",
              "name.given.first()",
              "name.given.join(',')",
              "name.given.where($this = 'Ann')",
              "name.given.exists()",
              "name.given[1]",
              "name.given.distinct()",
              "name.given.upper()",
              "name.given.length()",
              "name.given.select($this + '!')",
              "name.given.allTrue()",
              "name.given.last()",
              "name.given.toChars()",
              "name.given.isDistinct()",
              "name.given.combine(name.given).count()")) {
        try {
          final String view =
              "{\"resourceType\":\"ViewDefinition\",\"resource\":\"Patient\",\"status\":\"active\","
                  + "\"select\":[{\"column\":[{\"path\":\"id\",\"name\":\"id\"},"
                  + "{\"path\":\""
                  + path.replace("'", "\\u0027")
                  + "\",\"name\":\"result\",\"collection\":true}]}]}";
          final List<Row> rows = data.view("Patient").json(view).execute().collectAsList();
          System.out.println("  " + path + " -> " + rows);
        } catch (final Exception e) {
          System.out.println(
              "  " + path + " -> FAILED " + e.getClass().getSimpleName() + ": " + shorten(e));
        }
      }
    }
    spark.stop();
  }

  private static String shorten(final Exception e) {
    final String message = String.valueOf(e.getMessage());
    return message.length() > 300 ? message.substring(0, 300) : message;
  }
}
