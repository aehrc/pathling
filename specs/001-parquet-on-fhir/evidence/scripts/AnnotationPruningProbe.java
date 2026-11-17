import au.csiro.pathling.encoders.ColumnFunctions;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

/**
 * Asks whether Parquet reads only an annotation, in each placement decision 135 weighed, when it is
 * reached by the expressions the engine builds: ResolveOrNull for a field, then a filter that drops
 * nulls and a flatten where the result nests.
 *
 * <p>Both placements are in one file. The container placement keeps a primitive's annotations in its
 * `_x` group and a complex element's in its own structure. The sibling placement keeps them as
 * `__<element>_<annotation>` fields beside the element.
 */
public class AnnotationPruningProbe {

  static final String META = "id string, extension array<struct<url string, valueString string>>";
  static final String RANGE = "__start timestamp, __end timestamp";
  static final String CANON = "struct<value decimal(38,6), code string>";
  static final String QTY =
      "value string, comparator string, unit string, system string, code string, " + META;

  static final String SCHEMA =
      String.join(
          ", ",
          "id string",
          "birthDate string",
          "_birthDate struct<" + META + ", " + RANGE + ">",
          "__birthDate_start timestamp",
          "__birthDate_end timestamp",
          "given array<string>",
          "_given array<struct<" + META + ", " + RANGE + ">>",
          "__given_start array<timestamp>",
          "__given_end array<timestamp>",
          "valueQuantity struct<" + QTY + ", __canonical " + CANON + ">",
          "__valueQuantity_canonical " + CANON,
          "name array<struct<family string, given array<string>, _given array<struct<"
              + META
              + ", "
              + RANGE
              + ">>, __given_start array<timestamp>, __given_end array<timestamp>, big string>>",
          "component array<struct<code string, valueDateTime string, _valueDateTime struct<"
              + META
              + ", "
              + RANGE
              + ">, __valueDateTime_start timestamp, __valueDateTime_end timestamp,"
              + " valueQuantity struct<"
              + QTY
              + ", __canonical "
              + CANON
              + ">, __valueQuantity_canonical "
              + CANON
              + ", big string>>");

  static final String EXT = "[{\"url\":\"http://example.com/e\",\"valueString\":\"x\"}]";
  static final String RNG =
      "\"__start\":\"2020-01-01T00:00:00Z\",\"__end\":\"2020-12-31T23:59:59Z\"";

  static String row(final String id) {
    final String meta = "\"id\":\"m\",\"extension\":" + EXT + "," + RNG;
    final String canon = "{\"value\":1.5,\"code\":\"kg\"}";
    final String qty =
        "{\"value\":\"1500\",\"unit\":\"g\",\"system\":\"http://unitsofmeasure.org\",\"code\":\"g\","
            + "\"extension\":"
            + EXT
            + ",\"__canonical\":"
            + canon
            + "}";
    final String component =
        "{\"code\":\"c\",\"valueDateTime\":\"2020\",\"_valueDateTime\":{"
            + meta
            + "},\"__valueDateTime_start\":\"2020-01-01T00:00:00Z\","
            + "\"__valueDateTime_end\":\"2020-12-31T23:59:59Z\",\"valueQuantity\":"
            + qty
            + ",\"__valueQuantity_canonical\":"
            + canon
            + ",\"big\":\"b\"}";
    final String name =
        "{\"family\":\"f\",\"given\":[\"a\",\"b\"],\"_given\":[{"
            + meta
            + "},null],\"__given_start\":[\"2020-01-01T00:00:00Z\",null],"
            + "\"__given_end\":[\"2020-12-31T23:59:59Z\",null],\"big\":\"b\"}";
    return "{\"id\":\""
        + id
        + "\",\"birthDate\":\"2020\",\"_birthDate\":{"
        + meta
        + "},\"__birthDate_start\":\"2020-01-01T00:00:00Z\",\"__birthDate_end\":\"2020-12-31T23:59:59Z\","
        + "\"given\":[\"a\",\"b\"],\"_given\":[{"
        + meta
        + "},null],\"__given_start\":[\"2020-01-01T00:00:00Z\",null],"
        + "\"__given_end\":[\"2020-12-31T23:59:59Z\",null],\"valueQuantity\":"
        + qty
        + ",\"__valueQuantity_canonical\":"
        + canon
        + ",\"name\":["
        + name
        + ","
        + name
        + "],\"component\":["
        + component
        + ","
        + component
        + "]}";
  }

  /** Reads a field the way the engine's traversal does, given whether the input is an array. */
  static Column traverse(final Column input, final String field, final boolean array) {
    if (!array) {
      return ColumnFunctions.resolveOrNull(input, field, DataTypes.NullType);
    }
    return functions.filter(
        ColumnFunctions.resolveOrNull(input, field, DataTypes.createArrayType(DataTypes.NullType)),
        Column::isNotNull);
  }

  static Column flatten(final Column nested) {
    return functions.flatten(nested);
  }

  static String readSchema(final Dataset<Row> df) {
    final String plan = df.queryExecution().executedPlan().toString();
    final Matcher m = Pattern.compile("ReadSchema: (struct<.*?>)\\s*$", Pattern.MULTILINE).matcher(plan);
    String last = "<not found>";
    while (m.find()) {
      last = m.group(1);
    }
    return last;
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
    System.out.println("Spark " + spark.version());

    final Path dir = Files.createTempDirectory("annotation-pruning");
    final String out = dir.resolve("data").toString();
    spark
        .read()
        .schema(SCHEMA)
        .json(spark.createDataset(List.of(row("1"), row("2")), Encoders.STRING()))
        .write()
        .parquet(out);
    final Dataset<Row> df = spark.read().parquet(out);

    final Column c = functions.col("component");
    final Column n = functions.col("name");
    final Map<String, Column> cases = new LinkedHashMap<>();

    // A singular element.
    cases.put("S1 container: _birthDate.__start", traverse(functions.col("_birthDate"), "__start", false));
    cases.put("S2 sibling:   __birthDate_start", functions.col("__birthDate_start"));
    cases.put("S3 container: valueQuantity.__canonical",
        traverse(functions.col("valueQuantity"), "__canonical", false));
    cases.put("S4 sibling:   __valueQuantity_canonical", functions.col("__valueQuantity_canonical"));
    cases.put("S5 element:   valueQuantity.value",
        traverse(functions.col("valueQuantity"), "value", false));

    // A repeating primitive at the root.
    cases.put("R1 container: _given.__start", traverse(functions.col("_given"), "__start", true));
    cases.put("R2 sibling:   __given_start", functions.col("__given_start"));
    cases.put("R3 element:   given", functions.col("given"));

    // An element inside a repeating element, through the engine's traversal.
    cases.put("N1 container: component._valueDateTime.__start",
        traverse(traverse(c, "_valueDateTime", true), "__start", true));
    cases.put("N2 sibling:   component.__valueDateTime_start",
        traverse(c, "__valueDateTime_start", true));
    cases.put("N3 container: component.valueQuantity.__canonical",
        traverse(traverse(c, "valueQuantity", true), "__canonical", true));
    cases.put("N4 sibling:   component.__valueQuantity_canonical",
        traverse(c, "__valueQuantity_canonical", true));
    cases.put("N5 element:   component.valueDateTime", traverse(c, "valueDateTime", true));
    cases.put("N6 element:   component.valueQuantity.value",
        traverse(traverse(c, "valueQuantity", true), "value", true));

    // The same, without the filter between the steps.
    cases.put("U1 container, no filter: component._valueDateTime.__start",
        ColumnFunctions.resolveOrNull(
            ColumnFunctions.resolveOrNull(c, "_valueDateTime", DataTypes.NullType),
            "__start",
            DataTypes.NullType));
    cases.put("U2 container, no filter: component.valueQuantity.__canonical",
        ColumnFunctions.resolveOrNull(
            ColumnFunctions.resolveOrNull(c, "valueQuantity", DataTypes.NullType),
            "__canonical",
            DataTypes.NullType));

    // A repeating primitive inside a repeating element, which nests and is flattened.
    cases.put("P1 container: name._given.__start",
        traverse(flatten(traverse(n, "_given", true)), "__start", true));
    cases.put("P1u container, no filter: flatten(name._given).__start",
        ColumnFunctions.resolveOrNull(
            flatten(ColumnFunctions.resolveOrNull(n, "_given", DataTypes.NullType)),
            "__start",
            DataTypes.NullType));
    cases.put("P2 sibling:   name.__given_start", flatten(traverse(n, "__given_start", true)));
    cases.put("P3 element:   name.given", flatten(traverse(n, "given", true)));

    // Alignment: a repeating primitive and its annotation read together.
    cases.put("A1 container: zip(given, _given.__start)",
        functions.arrays_zip(functions.col("given"),
            ColumnFunctions.resolveOrNull(functions.col("_given"), "__start", DataTypes.NullType)));
    cases.put("A2 sibling:   zip(given, __given_start)",
        functions.arrays_zip(functions.col("given"), functions.col("__given_start")));

    for (final Map.Entry<String, Column> e : cases.entrySet()) {
      final Dataset<Row> q = df.select(e.getValue().as("v"));
      final String value = String.valueOf(q.selectExpr("to_json(named_struct('v', v))").first().get(0));
      System.out.printf("%n%s%n  ReadSchema: %s%n  value:      %s%n", e.getKey(), readSchema(q), value);
    }
    spark.stop();
  }
}
