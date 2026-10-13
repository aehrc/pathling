import au.csiro.pathling.benchmark.BenchmarkResources;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.DatasetSource;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * T136 probe: the facts used to attribute the benchmark movements, gathered on the benchmark's own
 * data and views. For each resource type it prints the leaf-field count of the schema the public
 * encode now writes and of the previous layout's encoder schema. For each view it prints the size
 * of the optimised plan, the number of canonical-quantity UDF calls in it, and whether the
 * executed plan is wholly code-generated.
 *
 * <p>Run with the shaded benchmark jar as the classpath: {@code java -cp
 * benchmark/target/benchmark-9.9.0.jar T136Probe.java}.
 */
public class T136Probe {

  private static final Map<String, String> VIEWS =
      Map.of(
          "ConditionFlat", "Condition",
          "EncounterFlat", "Encounter",
          "PatientAddresses", "Patient",
          "PatientAndContactAddressUnion", "Patient",
          "PatientDemographics", "Patient",
          "UsCoreBloodPressures", "Observation",
          "QuestionnaireResponseFlat", "QuestionnaireResponse");

  public static void main(final String[] args) throws Exception {
    final PathlingContext pc = BenchmarkResources.createPathlingContext("T136Probe");
    pc.getSpark().sparkContext().setLogLevel("ERROR");

    for (final String type :
        List.of("Patient", "Condition", "Encounter", "Observation", "QuestionnaireResponse")) {
      final Dataset<Row> encoded =
          pc.encode(BenchmarkResources.readResourceStrings(pc, type), type);
      final StructType previous = pc.getFhirEncoders().of(type).schema();
      System.out.printf(
          "SCHEMA %s new: %d top-level / %d leaves; previous: %d top-level / %d leaves%n",
          type,
          encoded.schema().fields().length,
          leaves(encoded.schema()),
          previous.fields().length,
          leaves(previous));
    }

    for (final Map.Entry<String, String> view : VIEWS.entrySet()) {
      final String type = view.getValue();
      final DatasetSource source = pc.read().datasets();
      source.dataset(type, pc.encode(BenchmarkResources.readResourceStrings(pc, type), type));
      final Path dir = BenchmarkResources.createTempDirectory("t136-probe-");
      source.write().delta(dir.toString());
      final QueryableDataSource delta = pc.read().delta(dir.toString());
      final String json;
      try (final InputStream in = BenchmarkResources.getResourceAsStream(view.getKey() + ".json")) {
        json = new String(in.readAllBytes(), StandardCharsets.UTF_8);
      }
      final Dataset<Row> result = delta.view(type).json(json).execute();
      final String optimised = result.queryExecution().optimizedPlan().toString();
      final String executed = result.queryExecution().executedPlan().toString();
      System.out.printf(
          "PLAN %s optimised chars: %d; canonical_quantity_value: %d; "
              + "canonical_quantity_code: %d; CASE WHEN: %d; decimal casts: %d; "
              + "executed plan has WholeStageCodegen: %b%n",
          view.getKey(),
          optimised.length(),
          count(optimised, "canonical_quantity_value"),
          count(optimised, "canonical_quantity_code"),
          count(optimised, "CASE WHEN"),
          count(optimised, "as decimal("),
          executed.contains("WholeStageCodegen") || executed.contains("*("));
      if (args.length > 0 && args[0].equals(view.getKey())) {
        System.out.println(optimised);
      }
    }
    pc.getSpark().stop();
  }

  private static int count(final String haystack, final String needle) {
    int n = 0;
    for (int i = haystack.indexOf(needle); i >= 0; i = haystack.indexOf(needle, i + 1)) {
      n++;
    }
    return n;
  }

  private static int leaves(final DataType type) {
    if (type instanceof StructType struct) {
      int n = 0;
      for (final StructField field : struct.fields()) {
        n += leaves(field.dataType());
      }
      return n;
    }
    if (type instanceof ArrayType array) {
      return leaves(array.elementType());
    }
    return 1;
  }
}
