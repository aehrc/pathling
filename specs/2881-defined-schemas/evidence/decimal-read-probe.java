/*
 * Probes which Spark type the inferred JSON read, with FhirJsonReader's options, gives a FHIR
 * decimal, and what is stored from it (tasks.md T003, research R3). Run with run-probe.sh.
 *
 * Each case is one dataset of Observation documents whose valueQuantity.value values differ. For
 * each, the probe prints the type that inference gives the value, with the options of
 * FhirJsonReader.json(spark), and the text FhirReader.json().read stores for each value. It then
 * parses the same documents with from_json and a schema that gives the value a fixed type, as a
 * defined read would, stores them with the same transform, and prints where that text differs
 * from what the inferred read stored.
 */
package au.csiro.pathling.io;

import au.csiro.pathling.definition.DefinitionContext;
import au.csiro.pathling.definition.fhir.FhirDefinitionContext;
import ca.uhn.fhir.context.FhirContext;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

class DecimalReadProbe {

  static final Map<String, String> FAIL_FAST = Map.of("mode", "FAILFAST");

  static SparkSession spark;
  static DefinitionContext definitions;

  public static void main(final String[] args) throws Exception {
    spark =
        SparkSession.builder()
            .master("local[2]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate();
    System.out.println("Spark " + spark.version());
    definitions = FhirDefinitionContext.of(FhirContext.forR4());

    probe(
        "fractional and integral mixed",
        List.of("1.10", "100", "0.1234567890123456789", "1e2", "-0.0"));
    probe("fractional only", List.of("1.10", "2.5"));
    probe("integral only", List.of("100", "7", "-3"));
    probe("integral only, one beyond a long", List.of("12345678901234567890123", "7"));
    probe(
        "integral beyond a long, mixed with a fraction", List.of("12345678901234567890123", "1.5"));

    spark.stop();
  }

  static void probe(final String label, final List<String> values) {
    System.out.println("\n--- " + label + ": " + values);
    final List<String> documents =
        values.stream()
            .map(
                value ->
                    "{\"resourceType\":\"Observation\",\"id\":\"o"
                        + values.indexOf(value)
                        + "\",\"status\":\"final\",\"code\":{\"text\":\"x\"},"
                        + "\"valueQuantity\":{\"value\":"
                        + value
                        + "}}")
            .toList();
    final Dataset<String> text = spark.createDataset(documents, Encoders.STRING());

    // The inferred read, with the options of FhirJsonReader.json(spark).
    final StructType inferred = spark.read().options(FAIL_FAST).json(text).schema();
    System.out.println("    inferred type: " + valueType(inferred).simpleString());
    final List<String> stored =
        stored(FhirReader.of(spark, definitions).json().read("Observation", text));
    System.out.println(
        "    stored type: "
            + valueType(FhirReader.of(spark, definitions).json().read("Observation", text).schema())
                .simpleString());
    System.out.println("    stored by the inferred read: " + stored);

    // The same resources as XML, through today's XML read, for FR-009.
    final List<String> xml =
        values.stream()
            .map(
                value ->
                    "<Observation xmlns=\"http://hl7.org/fhir\"><id value=\"o"
                        + values.indexOf(value)
                        + "\"/><status value=\"final\"/><code><text value=\"x\"/></code>"
                        + "<valueQuantity><value value=\""
                        + value
                        + "\"/></valueQuantity></Observation>")
            .toList();
    final List<String> xmlStored =
        stored(
            FhirReader.of(spark, definitions)
                .xml()
                .read("Observation", spark.createDataset(xml, Encoders.STRING())));
    System.out.println(
        "    stored by the XML read: "
            + xmlStored
            + (xmlStored.equals(stored) ? " (same as JSON)" : " (DIFFERS from JSON)"));

    // The same documents parsed with a fixed type for the value, as a defined read would.
    for (final DataType fixed :
        List.of(
            DataTypes.DoubleType,
            DataTypes.LongType,
            DataTypes.createDecimalType(38, 18),
            DataTypes.StringType)) {
      final StructType given = withValueType(inferred, fixed);
      try {
        final List<String> definedStored = stored(definedRead(text, given));
        System.out.println(
            "    given "
                + fixed.simpleString()
                + ": stored "
                + definedStored
                + (definedStored.equals(stored)
                    ? " (same as inferred)"
                    : " (DIFFERS from inferred)"));
      } catch (final Exception e) {
        final String message =
            String.valueOf(e.getMessage()).strip().lines().findFirst().orElse("");
        System.out.println(
            "    given "
                + fixed.simpleString()
                + ": FAILED: "
                + e.getClass().getSimpleName()
                + ": "
                + message.substring(0, Math.min(200, message.length())));
      }
    }
  }

  /** from_json with a given schema, then the same transform as FhirJsonReader.readBundles. */
  static Dataset<Row> definedRead(final Dataset<String> text, final StructType given) {
    return text.select(
            functions
                .from_json(functions.col("value"), given, FAIL_FAST)
                .alias(BundleResource.PARSED))
        .select(
            ResourceTransformer.of(definitions)
                .toLayout(
                    "Observation",
                    given,
                    functions.col(BundleResource.PARSED),
                    UnaryOperator.identity()));
  }

  static List<String> stored(final Dataset<Row> layout) {
    return layout.orderBy("id").select("valueQuantity.value").as(Encoders.STRING()).collectAsList();
  }

  static DataType valueType(final StructType schema) {
    return ((StructType) schema.apply("valueQuantity").dataType()).apply("value").dataType();
  }

  static StructType withValueType(final StructType schema, final DataType type) {
    final StructType quantity = new StructType().add("value", type);
    StructType out = new StructType();
    for (final var field : schema.fields()) {
      out =
          field.name().equals("valueQuantity")
              ? out.add("valueQuantity", quantity)
              : out.add(field);
    }
    return out;
  }
}
