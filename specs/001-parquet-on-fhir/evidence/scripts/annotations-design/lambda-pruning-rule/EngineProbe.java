import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import spike.CaseProbe;
import spike.LambdaPruning;

/**
 * Case 5: views through the real engine (library API), with the rule installed at runtime on the
 * existing session through spark.experimental().extraOptimizations. Usage: EngineProbe dataDir
 * ndjsonDir singularParquetDir [-v].
 */
public class EngineProbe {

  static String view(final String forEach, final String path) {
    final String column =
        "{\"column\":[{\"path\":\"" + path + "\",\"name\":\"r\",\"collection\":true}]";
    final String select =
        forEach == null ? column + "}" : column + ",\"forEach\":\"" + forEach + "\"}";
    return "{\"resourceType\":\"ViewDefinition\",\"resource\":\"Patient\","
        + "\"status\":\"active\",\"select\":[" + select + "]}";
  }

  static void run(final SparkSession spark, final QueryableDataSource data, final StructType full,
      final String label, final String forEach, final String path) {
    runView(spark, data, full, label, view(forEach, path));
  }

  static void runView(final SparkSession spark, final QueryableDataSource data,
      final StructType full, final String label, final String v) {
    LambdaPruning.uninstall(spark);
    final Dataset<Row> off = data.view("Patient").json(v).execute();
    final String offRead = CaseProbe.compact(CaseProbe.readSchema(off), full);
    final List<String> offRows = off.collectAsList().stream().map(Row::toString).sorted().toList();
    LambdaPruning.install(spark);
    final Dataset<Row> on = data.view("Patient").json(v).execute();
    final String onRead = CaseProbe.compact(CaseProbe.readSchema(on), full);
    final List<String> onRows = on.collectAsList().stream().map(Row::toString).sorted().toList();
    final boolean same = offRows.equals(onRows) && off.schema().equals(on.schema());
    System.out.printf("| %s | %s | %s | %s (%d rows) |%n", label, offRead, onRead,
        same ? "yes" : "**NO**", onRows.size());
    LambdaPruning.uninstall(spark);
  }

  public static void main(final String[] args) throws Exception {
    // An existing session, built without extensions, as PathlingContext.create(spark) receives.
    final SparkSession spark =
        SparkSession.builder()
            .master("local[2]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.adaptive.enabled", "false")
            .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
            .getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");
    LambdaPruning.verbose_$eq(Arrays.asList(args).contains("-v"));
    final PathlingContext ptc = PathlingContext.create(spark);
    final String stored = args[0] + "/engine";
    ptc.read().ndjson(args[1]).write().saveMode("overwrite").parquet(stored);
    final QueryableDataSource names = ptc.read().parquet(stored);
    final StructType namesFull = spark.read().parquet(stored + "/Patient.*.parquet").schema();
    System.out.println("Stored name type: "
        + namesFull.apply("name").dataType().simpleString().substring(0, 200) + "...");

    System.out.println("| Case | Reads without the rule | Reads with it | Results equal |");
    System.out.println("| --- | --- | --- | --- |");
    final String[][] nameCases = {
      {null, "name.family"},
      {null, "name.given"},
      {null, "name.period.start"},
      {null, "name.where(period.start > @1990).family"},
      {null, "name.where(use = 'nickname').given"},
      {null, "name.exists()"},
      {null, "name.count()"},
      {null, "address.period.start"},
      {null, "address.city | name.family"},
      {"name", "family"},
      {"name", "period.start"},
      {null, "name.first().family"},
      {null, "name.where(family.exists()).given"},
    };
    for (final String[] c : nameCases) {
      run(spark, names, namesFull, (c[0] == null ? "" : "forEach " + c[0] + " / ") + "`" + c[1]
          + "`", c[0], c[1]);
    }
    // A multi-column view: plain columns, a where clause, and a unionAll of two forEach selects.
    runView(spark, names, namesFull, "multi-column view with where and unionAll",
        "{\"resourceType\":\"ViewDefinition\",\"resource\":\"Patient\",\"status\":\"active\","
            + "\"select\":[{\"column\":[{\"path\":\"id\",\"name\":\"id\"},"
            + "{\"path\":\"birthDate\",\"name\":\"bd\"}]},"
            + "{\"unionAll\":[{\"forEachOrNull\":\"name\",\"column\":[{\"path\":\"family\",\"name\":\"v\"}]},"
            + "{\"forEach\":\"address\",\"column\":[{\"path\":\"city\",\"name\":\"v\"}]}]}],"
            + "\"where\":[{\"path\":\"name.where(use = 'nickname').exists()\"}]}");

    final QueryableDataSource singular = ptc.read().parquet(args[2]);
    final StructType singularFull = spark.read().parquet(args[2] + "/Patient.parquet").schema();
    final String[] singularCases = {
      "birthDate",
      "birthDate.where($this > @1990)",
      "birthDate | deceasedDateTime",
      "(birthDate | deceasedDateTime).where($this > @1990).exists()",
    };
    for (final String p : singularCases) {
      run(spark, singular, singularFull, "singular-meta small: `" + p + "`", null, p);
    }
    spark.stop();
  }
}
