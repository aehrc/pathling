import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.PlanRewrite;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import spike.CaseProbe;
import spike.LambdaPruning;

/**
 * Builds views through the real engine and compares three plans of each: the original with the
 * lambda-pruning rule off, the original with it on, and the original rewritten so that a Project
 * computes every Generate input beneath the Generate ("project before generate"), rule off.
 * Usage: ProbeMain dataDir ndjsonDir singularParquetDir.
 */
public class ProbeMain {

  static String view(final String select, final String where) {
    return "{\"resourceType\":\"ViewDefinition\",\"resource\":\"Patient\",\"status\":\"active\","
        + "\"select\":[" + select + "]"
        + (where == null ? "" : ",\"where\":[{\"path\":\"" + where + "\"}]") + "}";
  }

  static String col(final String path, final String name) {
    return "{\"path\":\"" + path + "\",\"name\":\"" + name + "\",\"collection\":true}";
  }

  static String simple(final String forEach, final String path) {
    final String column = "{\"column\":[" + col(path, "r") + "]";
    return view(forEach == null ? column + "}" : column + ",\"forEach\":\"" + forEach + "\"}", null);
  }

  static final StringBuilder DETAIL = new StringBuilder();

  static String reads(final Dataset<Row> df, final StructType full) {
    return CaseProbe.compact(CaseProbe.readSchema(df), full).replace(" (whole)", "");
  }

  static List<String> rows(final Dataset<Row> df) {
    return df.collectAsList().stream().map(Row::toString).sorted().toList();
  }

  static void runView(final SparkSession spark, final QueryableDataSource data,
      final StructType full, final String label, final String v) {
    try {
      LambdaPruning.uninstall(spark);
      final String cell = label.replace("|", "\\|");
      final Dataset<Row> off = data.view("Patient").json(v).execute();
      final String offRead = reads(off, full);
      final List<String> offRows = rows(off);
      final Dataset<Row> rw = PlanRewrite.rewrite(data.view("Patient").json(v).execute());
      final int generates = PlanRewrite.countRewritten(data.view("Patient").json(v).execute());
      final String rwRead = reads(rw, full);
      final List<String> rwRows = rows(rw);
      final String kept = PlanRewrite.optimisedGenerates(rw);
      // Both directions of except, to compare full rows rather than counts.
      final long minus1 = off.except(rw).count();
      final long minus2 = rw.except(off).count();
      LambdaPruning.install(spark);
      final Dataset<Row> on = data.view("Patient").json(v).execute();
      final String onRead = reads(on, full);
      final List<String> onRows = rows(on);
      LambdaPruning.uninstall(spark);
      final boolean same = offRows.equals(rwRows) && offRows.equals(onRows)
          && off.schema().equals(rw.schema()) && minus1 == 0 && minus2 == 0;
      final String cls = rwRead.equals(onRead) ? "A" : rwRead.equals(offRead) ? "C" : "B";
      System.out.printf("| %s | %s | %s | %s | %s | %s | %s (%d rows) |%n", cell, offRead, rwRead,
          onRead, cls, generates, same ? "yes" : "**NO**", offRows.size());
      DETAIL.append(String.format("%s: rewritten=%d; optimised generates: %s; schemaEqual=%s%n",
          label, generates, kept, off.schema().equals(rw.schema())));
    } catch (final Exception e) {
      LambdaPruning.uninstall(spark);
      System.out.printf("| %s | ERROR | %s |%n", label, String.valueOf(e).replace("\n", " "));
    }
  }

  public static void main(final String[] args) throws Exception {
    final SparkSession spark =
        SparkSession.builder()
            .master("local[2]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.adaptive.enabled", "false")
            .config("spark.sql.maxMetadataStringLength", "100000")
            .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
            .getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");
    LambdaPruning.verbose_$eq(false);
    final PathlingContext ptc = PathlingContext.create(spark);
    final String stored = args[0] + "/engine";
    ptc.read().ndjson(args[1]).write().saveMode("overwrite").parquet(stored);
    final QueryableDataSource names = ptc.read().parquet(stored);
    final StructType namesFull = spark.read().parquet(stored + "/Patient.*.parquet").schema();
    System.out.println("Spark " + spark.version());
    System.out.println("| View | Original, rule off | Rewritten, rule off | Original, rule on | "
        + "Class (auto) | Generates rewritten | Results equal |");
    System.out.println("| --- | --- | --- | --- | --- | --- | --- |");

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
      runView(spark, names, namesFull,
          (c[0] == null ? "" : "forEach " + c[0] + " / ") + "`" + c[1] + "`",
          simple(c[0], c[1]));
    }
    runView(spark, names, namesFull, "multi-column view with where and unionAll",
        "{\"resourceType\":\"ViewDefinition\",\"resource\":\"Patient\",\"status\":\"active\","
            + "\"select\":[{\"column\":[{\"path\":\"id\",\"name\":\"id\"},"
            + "{\"path\":\"birthDate\",\"name\":\"bd\"}]},"
            + "{\"unionAll\":[{\"forEachOrNull\":\"name\",\"column\":[{\"path\":\"family\",\"name\":\"v\"}]},"
            + "{\"forEach\":\"address\",\"column\":[{\"path\":\"city\",\"name\":\"v\"}]}]}],"
            + "\"where\":[{\"path\":\"name.where(use = 'nickname').exists()\"}]}");

    // Additional views.
    final String famCol = "\"column\":[{\"path\":\"family\",\"name\":\"f\"}]";
    runView(spark, names, namesFull, "nested forEach name / forEach given",
        view("{\"forEach\":\"name\",\"select\":[{" + famCol + "},{\"forEach\":\"given\","
            + "\"column\":[{\"path\":\"$this\",\"name\":\"g\"}]}]}", null));
    runView(spark, names, namesFull, "forEachOrNull name / family",
        view("{\"forEachOrNull\":\"name\"," + famCol + "}", null));
    runView(spark, names, namesFull, "forEach name / family, where on annotation-free leaf (use)",
        view("{\"forEach\":\"name\"," + famCol + "}", "name.where(use = 'nickname').exists()"));
    runView(spark, names, namesFull, "forEach name / family, where with exists(criteria)",
        view("{\"forEach\":\"name\"," + famCol + "}", "name.exists(use = 'nickname')"));
    runView(spark, names, namesFull, "forEach name / family, where first().family = 'F1' (annotation-free leaf)",
        view("{\"forEach\":\"name\"," + famCol + "}", "name.first().family = 'F1'"));
    runView(spark, names, namesFull, "unionAll of two forEach (name family, address city)",
        view("{\"unionAll\":[{\"forEach\":\"name\",\"column\":[{\"path\":\"family\",\"name\":\"v\"}]},"
            + "{\"forEach\":\"address\",\"column\":[{\"path\":\"city\",\"name\":\"v\"}]}]}", null));
    runView(spark, names, namesFull, "forEach name / family, where on period.start (annotated leaf)",
        view("{\"forEach\":\"name\"," + famCol + "}", "name.first().period.start > @1990"));
    runView(spark, names, namesFull, "forEach name / period.start compared in column",
        view("{\"forEach\":\"name\",\"column\":[{\"path\":\"period.start > @1990\",\"name\":\"c\"}]}",
            null));

    final QueryableDataSource singular = ptc.read().parquet(args[2]);
    final StructType singularFull = spark.read().parquet(args[2] + "/Patient.parquet").schema();
    final String[] singularCases = {
      "birthDate",
      "birthDate.where($this > @1990)",
      "birthDate | deceasedDateTime",
      "(birthDate | deceasedDateTime).where($this > @1990).exists()",
    };
    for (final String p : singularCases) {
      runView(spark, singular, singularFull, "singular-meta small: `" + p + "`", simple(null, p));
    }
    System.out.println();
    System.out.println(DETAIL);
    spark.stop();
  }
}
