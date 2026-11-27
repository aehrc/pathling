import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.SparkThrowable;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * How catalog-table appends (`saveAsTable`, as CatalogSink writes) behave when successive batches
 * of one resource type carry different schemas: narrower, wider, or reordered, at the top level,
 * inside a struct, and inside an array of structs.
 *
 * <p>Arguments: {@code <scratch dir> <in-memory|hive>}. Everything is printed to stdout.
 */
public class CatalogAppendProbe {

  static SparkSession spark;
  static int seq = 0;

  // The table's starting schema, written by the first batch:
  // id string, gender string, meta struct<versionId,source>, name array<struct<family,text>>.
  static final String BASE =
      "SELECT 'a' AS id, 'male' AS gender, named_struct('versionId','1','source','s1') AS meta,"
          + " array(named_struct('family','F1','text','T1')) AS name";

  static final String META = "named_struct('versionId','2','source','s2')";
  static final String NAME = "array(named_struct('family','F2','text','T2'))";

  // Each later batch, keyed by case. Every batch carries id 'b'.
  static final Map<String, String> CASES = new LinkedHashMap<>();

  static {
    CASES.put("1-narrow-top", "SELECT 'b' AS id, " + META + " AS meta, " + NAME + " AS name");
    CASES.put(
        "2s-narrow-struct",
        "SELECT 'b' AS id, 'female' AS gender, named_struct('versionId','2') AS meta, "
            + NAME
            + " AS name");
    CASES.put(
        "2a-narrow-array",
        "SELECT 'b' AS id, 'female' AS gender, "
            + META
            + " AS meta, array(named_struct('family','F2')) AS name");
    CASES.put(
        "3-wide-top",
        "SELECT 'b' AS id, 'female' AS gender, "
            + META
            + " AS meta, "
            + NAME
            + " AS name, '2000-01-01' AS birthDate");
    CASES.put(
        "4s-wide-struct",
        "SELECT 'b' AS id, 'female' AS gender,"
            + " named_struct('versionId','2','source','s2','lastUpdated','2024') AS meta, "
            + NAME
            + " AS name");
    CASES.put(
        "4a-wide-array",
        "SELECT 'b' AS id, 'female' AS gender, "
            + META
            + " AS meta, array(named_struct('family','F2','text','T2','use','official')) AS name");
    // Mixed: narrower and wider at once, at the same level.
    CASES.put(
        "Mt-mixed-top",
        "SELECT 'b' AS id, "
            + META
            + " AS meta, "
            + NAME
            + " AS name, '2000-01-01' AS birthDate");
    CASES.put(
        "Ms-mixed-struct",
        "SELECT 'b' AS id, 'female' AS gender,"
            + " named_struct('versionId','2','lastUpdated','2024') AS meta, "
            + NAME
            + " AS name");
    CASES.put(
        "Ma-mixed-array",
        "SELECT 'b' AS id, 'female' AS gender, "
            + META
            + " AS meta, array(named_struct('family','F2','use','official')) AS name");
    CASES.put(
        "8s-reorder-struct",
        "SELECT 'b' AS id, 'female' AS gender, named_struct('source','s2','versionId','2') AS meta, "
            + NAME
            + " AS name");
    CASES.put(
        "8a-reorder-array",
        "SELECT 'b' AS id, 'female' AS gender, "
            + META
            + " AS meta, array(named_struct('text','T2','family','F2')) AS name");
    CASES.put(
        "8t-reorder-top",
        "SELECT " + NAME + " AS name, " + META + " AS meta, 'female' AS gender, 'b' AS id");
  }

  static final String WIDE_ALL =
      "SELECT 'w' AS id, 'female' AS gender,"
          + " named_struct('versionId','2','source','s2','lastUpdated','2024') AS meta,"
          + " array(named_struct('family','F2','text','T2','use','official')) AS name,"
          + " '2000-01-01' AS birthDate";
  static final String NARROW_ALL =
      "SELECT 'n' AS id, named_struct('versionId','3') AS meta,"
          + " array(named_struct('family','F3')) AS name";

  public static void main(final String[] args) throws Exception {
    final Path scratch = Files.createDirectories(Path.of(args[0]));
    final String catalog = args.length > 1 ? args[1] : "in-memory";
    final Path dir = Files.createTempDirectory(scratch, "run-");
    SparkSession.Builder b =
        SparkSession.builder()
            .master("local[2]")
            .appName("catalog-append-probe")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.bindAddress", "localhost")
            .config("spark.driver.host", "localhost")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.warehouse.dir", dir.resolve("warehouse").toString())
            .config("spark.sql.catalogImplementation", catalog);
    if (catalog.equals("hive")) {
      b =
          b.config(
              "javax.jdo.option.ConnectionURL",
              "jdbc:derby:" + dir.resolve("metastore_db") + ";create=true");
    }
    spark = b.getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");
    System.out.println("Spark " + spark.version() + ", catalog " + catalog);
    System.out.println(
        "Delta jar "
            + org.apache.spark.sql.delta.catalog.DeltaCatalog.class
                .getProtectionDomain()
                .getCodeSource()
                .getLocation()
                .getPath()
                .replaceAll(".*/", ""));
    System.out.println("spark.sql.sources.default = " + spark.conf().get("spark.sql.sources.default"));
    System.out.println(
        "spark.sql.storeAssignmentPolicy = " + spark.conf().get("spark.sql.storeAssignmentPolicy"));
    System.out.println("spark.sql.ansi.enabled = " + spark.conf().get("spark.sql.ansi.enabled"));

    // Cases 1-5, 8 and 9: one fresh table per (mode, case, mergeSchema).
    for (final String mode : List.of("parquet", "delta", "default", "delta-path")) {
      for (final Map.Entry<String, String> c : CASES.entrySet()) {
        for (final boolean merge : List.of(false, true)) {
          appendCase(mode, c.getKey(), c.getValue(), merge, dir);
        }
      }
    }
    // Case 6: files written straight into a Parquet table's location.
    bypassCases(dir);
    // Case 7: ALTER TABLE.
    for (final String fmt : List.of("parquet", "delta")) {
      alterCases(fmt);
    }
    // Case 10: other public write routes.
    for (final String fmt : List.of("parquet", "delta")) {
      otherRoutes(fmt);
    }
    spark.stop();
  }

  static void appendCase(
      final String mode, final String label, final String sql, final boolean merge, final Path dir) {
    final String header = "[" + mode + "] " + label + " mergeSchema=" + merge;
    final String table = "t" + (++seq);
    final String path = dir.resolve("path-" + seq).toString();
    try {
      write(spark.sql(BASE), mode, table, path, "errorifexists", false);
    } catch (final Exception e) {
      System.out.println(header + "\n  BASE WRITE FAILED " + describe(e));
      return;
    }
    String outcome;
    try {
      write(spark.sql(sql), mode, table, path, "append", merge);
      outcome = "OK";
    } catch (final Exception e) {
      outcome = "FAIL " + describe(e);
    }
    System.out.println(header + "\n  append: " + outcome);
    final String provider = mode.equals("delta-path") ? "delta(path)" : provider(table);
    System.out.println("  provider: " + provider);
    show(mode.equals("delta-path") ? () -> spark.read().format("delta").load(path) : () -> spark.table(table));
  }

  static void write(
      final Dataset<Row> df,
      final String mode,
      final String table,
      final String path,
      final String saveMode,
      final boolean merge) {
    final DataFrameWriter<Row> w = df.write().mode(saveMode);
    if (merge) {
      w.option("mergeSchema", "true");
    }
    switch (mode) {
      case "parquet" -> w.format("parquet").saveAsTable(table);
      case "delta" -> w.format("delta").saveAsTable(table);
      case "default" -> w.saveAsTable(table);
      case "delta-path" -> w.format("delta").save(path);
      default -> throw new IllegalArgumentException(mode);
    }
  }

  interface Reader {
    Dataset<Row> read();
  }

  static void show(final Reader r) {
    try {
      final Dataset<Row> df = r.read();
      System.out.println("  schema: " + df.schema().simpleString());
      final List<String> rows = df.orderBy("id").toJSON().collectAsList();
      System.out.println("  rows (" + rows.size() + "):");
      rows.forEach(x -> System.out.println("    " + x));
    } catch (final Exception e) {
      System.out.println("  read FAIL " + describe(e));
    }
  }

  static String provider(final String table) {
    try {
      return spark.sql("DESCRIBE TABLE EXTENDED " + table).collectAsList().stream()
          .filter(r -> "Provider".equals(r.getString(0)))
          .map(r -> r.getString(1))
          .findFirst()
          .orElse("?");
    } catch (final Exception e) {
      return "? " + describe(e);
    }
  }

  static String location(final String table) {
    return spark.sql("DESCRIBE TABLE EXTENDED " + table).collectAsList().stream()
        .filter(r -> "Location".equals(r.getString(0)))
        .map(r -> r.getString(1))
        .findFirst()
        .orElseThrow();
  }

  static String describe(final Throwable e) {
    final String cls = e.getClass().getName();
    final String condition =
        e instanceof SparkThrowable st && st.getCondition() != null ? " [" + st.getCondition() + "]" : "";
    final String msg = e.getMessage() == null ? "" : e.getMessage().replaceAll("\\s+", " ");
    return cls + condition + ": " + (msg.length() > 600 ? msg.substring(0, 600) + "..." : msg);
  }

  static void step(final String label, final Runnable r) {
    try {
      r.run();
      System.out.println("  " + label + ": OK");
    } catch (final Exception e) {
      System.out.println("  " + label + ": FAIL " + describe(e));
    }
  }

  static void sql(final String s) {
    spark.sql(s).collect();
  }

  static final String DDL =
      "id STRING, gender STRING, meta STRUCT<versionId:STRING,source:STRING>,"
          + " name ARRAY<STRUCT<family:STRING,text:STRING>>";

  static void bypassCases(final Path dir) {
    // 6a: external table, schema declared, OPTIONS (mergeSchema 'true').
    final String locA = dir.resolve("ext-a").toString();
    System.out.println("[case 6a] external Parquet table, declared schema, OPTIONS (mergeSchema 'true')");
    step("write base files", () -> spark.sql(BASE).write().parquet(locA));
    step(
        "create table",
        () ->
            sql(
                "CREATE TABLE e6a ("
                    + DDL
                    + ") USING parquet OPTIONS (mergeSchema 'true') LOCATION '"
                    + locA
                    + "'"));
    bypassTail("e6a", locA);

    // 6b: external table, schema inferred at creation, OPTIONS (mergeSchema 'true').
    final String locB = dir.resolve("ext-b").toString();
    System.out.println("[case 6b] external Parquet table, inferred schema, OPTIONS (mergeSchema 'true')");
    step("write base files", () -> spark.sql(BASE).write().parquet(locB));
    step(
        "create table",
        () ->
            sql("CREATE TABLE e6b USING parquet OPTIONS (mergeSchema 'true') LOCATION '" + locB + "'"));
    bypassTail("e6b", locB);
    System.out.println("  -- drop and re-create the external table over the same location");
    step("drop table", () -> sql("DROP TABLE e6b"));
    step(
        "re-create table",
        () ->
            sql("CREATE TABLE e6b USING parquet OPTIONS (mergeSchema 'true') LOCATION '" + locB + "'"));
    show(() -> spark.table("e6b"));
    // Controls: is the merge on re-creation due to the table option, or to the session setting?
    System.out.println("  -- control: re-create without the mergeSchema option (session default)");
    System.out.println(
        "  spark.sql.parquet.mergeSchema = " + spark.conf().get("spark.sql.parquet.mergeSchema"));
    step("drop table", () -> sql("DROP TABLE e6b"));
    step("re-create table", () -> sql("CREATE TABLE e6b USING parquet LOCATION '" + locB + "'"));
    show(() -> spark.table("e6b"));
    System.out.println(
        "  -- control: re-create without the option, spark.sql.parquet.mergeSchema=true on the session");
    spark.conf().set("spark.sql.parquet.mergeSchema", "true");
    step("drop table", () -> sql("DROP TABLE e6b"));
    step("re-create table", () -> sql("CREATE TABLE e6b USING parquet LOCATION '" + locB + "'"));
    show(() -> spark.table("e6b"));
    spark.conf().unset("spark.sql.parquet.mergeSchema");

    // 6c: external table via saveAsTable with a path option and mergeSchema option.
    final String locC = dir.resolve("ext-c").toString();
    System.out.println("[case 6c] external Parquet table via saveAsTable(path, mergeSchema option)");
    step(
        "saveAsTable",
        () ->
            spark
                .sql(BASE)
                .write()
                .format("parquet")
                .option("path", locC)
                .option("mergeSchema", "true")
                .saveAsTable("e6c"));
    bypassTail("e6c", locC);

    // 6d: managed table via saveAsTable with mergeSchema option.
    System.out.println("[case 6d] managed Parquet table via saveAsTable(mergeSchema option)");
    step(
        "saveAsTable",
        () ->
            spark
                .sql(BASE)
                .write()
                .format("parquet")
                .option("mergeSchema", "true")
                .saveAsTable("m6d"));
    final String locD = location("m6d").replaceFirst("^file:", "");
    System.out.println("  location: " + locD);
    bypassTail("m6d", locD);
  }

  static void bypassTail(final String table, final String loc) {
    try {
      System.out.println(
          "  SHOW CREATE TABLE: "
              + spark.sql("SHOW CREATE TABLE " + table).first().getString(0).replaceAll("\\s+", " "));
    } catch (final Exception e) {
      System.out.println("  SHOW CREATE TABLE: FAIL " + describe(e));
    }
    System.out.println("  -- before:");
    show(() -> spark.table(table));
    step("append wide files directly", () -> spark.sql(WIDE_ALL).write().mode("append").parquet(loc));
    step("append narrow files directly", () -> spark.sql(NARROW_ALL).write().mode("append").parquet(loc));
    step("REFRESH TABLE", () -> sql("REFRESH TABLE " + table));
    System.out.println("  -- after REFRESH TABLE:");
    show(() -> spark.table(table));
    step("spark.catalog.refreshTable", () -> spark.catalog().refreshTable(table));
    System.out.println("  -- after spark.catalog.refreshTable:");
    show(() -> spark.table(table));
    step("spark.catalog.recoverPartitions", () -> spark.catalog().recoverPartitions(table));
    show(() -> spark.table(table));
    System.out.println("  -- reference: spark.read.option(mergeSchema,true).parquet(location):");
    show(() -> spark.read().option("mergeSchema", "true").parquet(loc));
  }

  static final List<String> ALTERS =
      Arrays.asList(
          "ADD COLUMNS (birthDate STRING)",
          "ADD COLUMNS (meta.lastUpdated STRING)",
          "ADD COLUMNS (name.element.use STRING)",
          "REPLACE COLUMNS (id STRING, gender STRING,"
              + " meta STRUCT<versionId:STRING,source:STRING,lastUpdated:STRING>,"
              + " name ARRAY<STRUCT<family:STRING,text:STRING,use:STRING>>, birthDate STRING)",
          "ALTER COLUMN meta TYPE STRUCT<versionId:STRING,source:STRING,lastUpdated:STRING>",
          "CHANGE COLUMN meta meta STRUCT<versionId:STRING,source:STRING,lastUpdated:STRING>");

  static void alterCases(final String fmt) {
    for (final String alter : ALTERS) {
      final String table = "a" + (++seq);
      System.out.println("[case 7 " + fmt + "] ALTER TABLE ... " + alter);
      try {
        spark.sql(BASE).write().format(fmt).saveAsTable(table);
      } catch (final Exception e) {
        System.out.println("  BASE WRITE FAILED " + describe(e));
        continue;
      }
      final boolean[] ok = {false};
      step(
          "alter",
          () -> {
            sql("ALTER TABLE " + table + " " + alter);
            ok[0] = true;
          });
      show(() -> spark.table(table));
      if (ok[0]) {
        step(
            "append 3-wide-top via saveAsTable",
            () ->
                spark
                    .sql(CASES.get("3-wide-top"))
                    .write()
                    .mode("append")
                    .format(fmt)
                    .saveAsTable(table));
        // Append a batch that is wider in every place the ALTER could have widened, and also
        // narrower than the original, to see whether the altered table now absorbs it.
        step(
            "append wide-all via saveAsTable",
            () -> spark.sql(WIDE_ALL).write().mode("append").format(fmt).saveAsTable(table));
        step(
            "append narrow-all via saveAsTable",
            () -> spark.sql(NARROW_ALL).write().mode("append").format(fmt).saveAsTable(table));
        show(() -> spark.table(table));
      }
    }
  }

  static final List<String> NARROW =
      List.of("1-narrow-top", "2s-narrow-struct", "2a-narrow-array");
  static final List<String> WIDE = List.of("3-wide-top", "4s-wide-struct", "4a-wide-array");

  static void insertByName(final String table, final Dataset<Row> batch) {
    batch.createOrReplaceTempView("batch");
    sql("INSERT INTO " + table + " BY NAME SELECT * FROM batch");
  }

  static void otherRoutes(final String fmt) {
    for (final String c : NARROW) {
      route(fmt, c, "INSERT INTO t BY NAME", CatalogAppendProbe::insertByName);
      route(fmt, c, "writeTo(t).append()", (t, df) -> df.writeTo(t).append());
      // The writer pads the batch to the table's schema before appending.
      route(
          fmt,
          c,
          "conform: table.limit(0).unionByName(batch, true), then saveAsTable append",
          (t, df) ->
              spark
                  .table(t)
                  .limit(0)
                  .unionByName(df, true)
                  .write()
                  .mode("append")
                  .format(fmt)
                  .saveAsTable(t));
    }
    for (final String c : WIDE) {
      route(fmt, c, "writeTo(t).append()", (t, df) -> df.writeTo(t).append());
      route(
          fmt,
          c,
          "writeTo(t).option(mergeSchema,true).append()",
          (t, df) -> df.writeTo(t).option("mergeSchema", "true").append());
      route(fmt, c, "INSERT INTO t BY NAME", CatalogAppendProbe::insertByName);
    }
  }

  interface Route {
    void append(String table, Dataset<Row> batch) throws Exception;
  }

  static void route(final String fmt, final String c, final String label, final Route r) {
    final String table = "r" + (++seq);
    System.out.println("[case 10 " + fmt + "] " + c + " via " + label);
    try {
      spark.sql(BASE).write().format(fmt).saveAsTable(table);
    } catch (final Exception e) {
      System.out.println("  BASE WRITE FAILED " + describe(e));
      return;
    }
    step(
        "append",
        () -> {
          try {
            r.append(table, spark.sql(CASES.get(c)));
          } catch (final RuntimeException e) {
            throw e;
          } catch (final Exception e) {
            throw new RuntimeException(e);
          }
        });
    show(() -> spark.table(table));
  }
}
