package spike;

import static org.apache.spark.sql.functions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Throwaway spike for issue #2881: Spark 4.0.2 / Delta 4.0.1 nullability behaviour, using only the
 * public API (Dataset, Column, functions, DataFrameReader/Writer). Each experiment prints its
 * label, the resulting schema (treeString shows nullable / containsNull / valueContainsNull), and
 * either the collected rows or the exception class and message.
 */
public class NullabilitySpike {

  static SparkSession spark;
  static Path tmp;

  // Shapes: top-level id, nested s.x inside a nullable struct s, array elements, map values.
  static final StructType INNER_NN =
      new StructType(new StructField[] {field("x", DataTypes.IntegerType, false)});
  static final StructType NN =
      new StructType(
          new StructField[] {
            field("id", DataTypes.StringType, false),
            field("s", INNER_NN, true),
            field("arr", DataTypes.createArrayType(DataTypes.IntegerType, false), true),
            field(
                "m",
                DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType, false),
                true)
          });
  static final StructType NULLABLE = (StructType) NN.asNullable();

  static StructField field(String n, org.apache.spark.sql.types.DataType t, boolean nullable) {
    return new StructField(n, t, nullable, Metadata.empty());
  }

  // Good row; then one row per kind of null: top-level id, s.x (parent present), s absent
  // (allowed),
  // array element, map value.
  static final String GOOD = "{\"id\":\"a\",\"s\":{\"x\":1},\"arr\":[1,2],\"m\":{\"k\":\"v\"}}";
  static final List<String> BAD_ROWS =
      List.of(
          "{\"s\":{\"x\":1},\"arr\":[1],\"m\":{\"k\":\"v\"}}", // id null
          "{\"id\":\"b\",\"s\":{},\"arr\":[1],\"m\":{\"k\":\"v\"}}", // s.x null, s present
          "{\"id\":\"c\",\"arr\":[1],\"m\":{\"k\":\"v\"}}", // s absent: must be allowed
          "{\"id\":\"d\",\"s\":{\"x\":1},\"arr\":[1,null],\"m\":{\"k\":\"v\"}}", // array element
                                                                                 // null
          "{\"id\":\"e\",\"s\":{\"x\":1},\"arr\":[1],\"m\":{\"k\":null}}" // map value null
          );

  public static void main(String[] args) throws Exception {
    tmp = Files.createTempDirectory("nullspike");
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("nullability-spike")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.warehouse.dir", tmp.resolve("wh").toString())
            .getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");

    final String only = args.length > 0 ? args[0] : "all";
    // Each bad row on its own, so that each null kind is observed separately.
    for (int i = 0; i < BAD_ROWS.size(); i++) {
      writeJson("bad" + i, List.of(GOOD, BAD_ROWS.get(i)));
    }
    writeJson("good", List.of(GOOD, GOOD.replace("\"a\"", "\"z\"")));

    if (only.equals("all") || only.equals("q1")) q1Native();
    if (only.equals("all") || only.equals("q2")) q2Narrowing();
    if (only.equals("all") || only.equals("q2s")) q2Streaming();
    if (only.equals("all") || only.equals("q3")) q3Widening();
    if (only.equals("all") || only.equals("q4")) q4Delta();
    if (only.equals("all") || only.equals("q5")) q5FalseFlag();
    if (only.equals("all") || only.equals("q6")) q6Guard();
    spark.stop();
  }

  // ---------------------------------------------------------------------------------------------
  // Q1: what Spark does natively with NOT NULL in a user schema.

  static void q1Native() throws Exception {
    h1("Q1 native behaviour");

    for (int i = 0; i < BAD_ROWS.size(); i++) {
      final String p = path("bad" + i);
      final int n = i;
      exp("json read schema(NN) bad" + n, () -> spark.read().schema(NN).json(p));
    }

    // Parquet: store the nullable data, read back with the NN schema.
    for (int i = 0; i < BAD_ROWS.size(); i++) {
      final String pq = tmp.resolve("pq" + i).toString();
      spark.read().schema(NULLABLE).json(path("bad" + i)).write().parquet(pq);
      final int n = i;
      exp("parquet read schema(NN) bad" + n, () -> spark.read().schema(NN).parquet(pq));
    }

    // Delta with a user-specified schema.
    final String dt = tmp.resolve("delta_q1").toString();
    spark.read().schema(NULLABLE).json(path("bad0")).write().format("delta").save(dt);
    exp("delta read schema(NN)", () -> spark.read().format("delta").schema(NN).load(dt));
    exp(
        "delta read schema(NULLABLE)",
        () -> spark.read().format("delta").schema(NULLABLE).load(dt));

    // from_json.
    for (int i = 0; i < BAD_ROWS.size(); i++) {
      final String p = path("bad" + i);
      final int n = i;
      exp(
          "from_json(NN) bad" + n,
          () -> spark.read().text(p).select(from_json(col("value"), NN).as("r")).select("r.*"));
    }

    // createDataFrame(List<Row>, schema).
    exp(
        "createDataFrame(rows, NN) with null id",
        () ->
            spark.createDataFrame(
                List.of(
                    RowFactory.create("a", null, null, null),
                    RowFactory.create(null, null, null, null)),
                NN));
    exp(
        "createDataFrame(rows, NN) with null array element",
        () ->
            spark.createDataFrame(
                List.of(
                    RowFactory.create(
                        "a",
                        null,
                        scala.jdk.javaapi.CollectionConverters.asScala(Arrays.asList(1, null))
                            .toSeq(),
                        null)),
                NN));
  }

  // ---------------------------------------------------------------------------------------------
  // Q2: narrowing with the public API.

  static Dataset<Row> badNullable(int i) {
    return spark.read().schema(NULLABLE).json(path("bad" + i));
  }

  static void q2Narrowing() throws Exception {
    h1("Q2 narrowing (batch)");

    exp(
        "raise_error alone (nullability of raise_error)",
        () -> badNullable(0).select(raise_error(lit("x")).as("r")).limit(0));
    exp(
        "coalesce(id, raise_error)",
        () -> badNullable(0).select(coalesce(col("id"), raise_error(lit("id is null"))).as("id")));
    exp(
        "when(id isNull, raise_error).otherwise(id)",
        () ->
            badNullable(0)
                .select(
                    when(col("id").isNull(), raise_error(lit("id is null")))
                        .otherwise(col("id"))
                        .as("id")));
    exp("Dataset.to(NN) on nullable data, bad0", () -> badNullable(0).to(NN));
    exp(
        "Dataset.to(NN) on nullable data, good",
        () -> spark.read().schema(NULLABLE).json(path("good")).to(NN));
    exp(
        "cast arr to array<int,containsNull=false>",
        () ->
            badNullable(3)
                .select(col("arr").cast(DataTypes.createArrayType(DataTypes.IntegerType, false))));
    exp(
        "ifnull/nvl(id, raise_error)",
        () -> badNullable(0).select(expr("nvl(id, raise_error('id is null'))").as("id")));

    // createDataFrame(rdd, schema).
    for (int i = 0; i < BAD_ROWS.size(); i++) {
      final int n = i;
      exp(
          "createDataFrame(javaRDD, NN) bad" + n,
          () -> spark.createDataFrame(badNullable(n).javaRDD(), NN));
    }

    // The "checked coalesce": the check raises before the non-null fallback can be reached, and the
    // non-null fallback makes Catalyst infer nullable = false.
    exp(
        "checked coalesce top-level, good",
        () ->
            spark
                .read()
                .schema(NULLABLE)
                .json(path("good"))
                .select(Narrow.top("id", DataTypes.StringType)));
    for (int i = 0; i < BAD_ROWS.size(); i++) {
      final int n = i;
      exp("Narrow.apply(NN) bad" + n, () -> Narrow.apply(badNullable(n), NN));
    }
    exp(
        "Narrow.apply(NN) good",
        () -> Narrow.apply(spark.read().schema(NULLABLE).json(path("good")), NN));
    exp(
        "Narrow.apply(NN) good then .to(NN) (shape check)",
        () -> Narrow.apply(spark.read().schema(NULLABLE).json(path("good")), NN).to(NN));
    // Does the optimiser keep the check when the column is then filtered on / not selected?
    exp(
        "Narrow.apply(NN) bad0 then select only s (id check pruned?)",
        () -> Narrow.apply(badNullable(0), NN).select("s"));
    exp(
        "Narrow.apply(NN) bad0 then count()",
        () ->
            spark.createDataFrame(
                List.of(RowFactory.create(Narrow.apply(badNullable(0), NN).count())),
                new StructType().add("count", DataTypes.LongType)));
    exp(
        "Narrow.apply(NN) bad0 filter(id isNull)",
        () -> Narrow.apply(badNullable(0), NN).filter(col("id").isNull()));
    // Plan check: is the guard visible in the optimised plan?
    System.out.println("--- optimised plan of Narrow.apply(NN) ---");
    System.out.println(
        Narrow.apply(badNullable(0), NN).queryExecution().optimizedPlan().treeString());
  }

  static void q2Streaming() throws Exception {
    h1("Q2/Q3 streaming");
    for (int i = 0; i < BAD_ROWS.size(); i++) {
      final int n = i;
      streamExp(
          "stream readStream.schema(NN) raw, bad" + n,
          () -> spark.readStream().schema(NN).json(path("bad" + n)));
      streamExp(
          "stream Narrow.apply(NN), bad" + n,
          () -> Narrow.apply(spark.readStream().schema(NULLABLE).json(path("bad" + n)), NN));
    }
    streamExp(
        "stream Narrow.apply(NN).to(NN), good",
        () -> Narrow.apply(spark.readStream().schema(NULLABLE).json(path("good")), NN).to(NN));
    streamExp(
        "stream .to(NN) on nullable, good",
        () -> spark.readStream().schema(NULLABLE).json(path("good")).to(NN));
    streamExp(
        "stream Widen.apply(Narrow.apply(..), NULLABLE), good",
        () ->
            Widen.apply(
                Narrow.apply(spark.readStream().schema(NULLABLE).json(path("good")), NN),
                NULLABLE));
    streamExp(
        "stream Narrow -> .to(NULLABLE) (widen by to), good",
        () ->
            Narrow.apply(spark.readStream().schema(NULLABLE).json(path("good")), NN).to(NULLABLE));
  }

  // ---------------------------------------------------------------------------------------------
  // Q3: widening with the public API.

  static Dataset<Row> nnGood() {
    return Narrow.apply(spark.read().schema(NULLABLE).json(path("good")), NN);
  }

  static void q3Widening() throws Exception {
    h1("Q3 widening (batch)");
    exp("source: Narrow.apply(good)", NullabilitySpike::nnGood);
    exp("Dataset.to(NULLABLE)", () -> nnGood().to(NULLABLE));
    exp("Widen.apply(NULLABLE)", () -> Widen.apply(nnGood(), NULLABLE));
    exp(
        "createDataFrame(javaRDD, NULLABLE)",
        () -> spark.createDataFrame(nnGood().javaRDD(), NULLABLE));
    exp(
        "cast to NULLABLE struct of whole row",
        () -> nnGood().select(struct(col("*")).cast(NULLABLE).as("r")).select("r.*"));
  }

  // ---------------------------------------------------------------------------------------------
  // Q4: Delta writes.

  static StructType schemaOf(String p) {
    return spark.read().format("delta").load(p).schema();
  }

  static void q4Delta() throws Exception {
    h1("Q4 Delta writes");
    // a) append NOT NULL-marked data into a nullable table.
    final String a = tmp.resolve("d_a").toString();
    spark.read().schema(NULLABLE).json(path("good")).write().format("delta").save(a);
    System.out.println("table a before:\n" + schemaOf(a).treeString());
    run(
        "append NN-marked into nullable table",
        () -> {
          nnGood().write().format("delta").mode("append").save(a);
          return null;
        });
    System.out.println("table a after:\n" + schemaOf(a).treeString());

    // c) create new table from NN-marked dataset (path and catalog).
    final String c = tmp.resolve("d_c").toString();
    run(
        "create table (path) from NN-marked dataset",
        () -> {
          nnGood().write().format("delta").save(c);
          return null;
        });
    System.out.println("table c (path) schema:\n" + schemaOf(c).treeString());
    run(
        "saveAsTable from NN-marked dataset",
        () -> {
          nnGood().write().format("delta").saveAsTable("t_c");
          return null;
        });
    System.out.println("t_c schema:\n" + spark.table("t_c").schema().treeString());
    // Is there a Delta table-creation path that keeps NOT NULL from the dataset? Try DeltaTable
    // builder with the dataset's schema (public Delta API).
    run(
        "DeltaTable.create().addColumns(NN-marked schema)",
        () -> {
          io.delta.tables.DeltaTable.create(spark)
              .tableName("t_builder")
              .addColumns(nnGood().schema())
              .execute();
          System.out.println(
              "t_builder schema:\n" + spark.table("t_builder").schema().treeString());
          return null;
        });
    run(
        "append NN-marked dataset into DDL NOT NULL table (t_builder)",
        () -> {
          nnGood().write().format("delta").mode("append").saveAsTable("t_builder");
          System.out.println("t_builder after:\n" + spark.table("t_builder").schema().treeString());
          return null;
        });
    run(
        "append Narrow(bad0) into t_builder",
        () -> {
          Narrow.apply(badNullable(0), NN)
              .write()
              .format("delta")
              .mode("append")
              .saveAsTable("t_builder");
          return null;
        });
    run(
        "append Narrow(bad2) (s absent, valid) into t_builder",
        () -> {
          Narrow.apply(badNullable(2), NN)
              .write()
              .format("delta")
              .mode("append")
              .saveAsTable("t_builder");
          return null;
        });
    run(
        "append nullable good (no nulls) into t_builder",
        () -> {
          spark
              .read()
              .schema(NULLABLE)
              .json(path("good"))
              .write()
              .format("delta")
              .mode("append")
              .saveAsTable("t_builder");
          return null;
        });
    for (int i : new int[] {3, 4}) {
      run(
          "append nullable bad" + i + " into t_builder (containsNull=false table)",
          () -> {
            badNullable(i).write().format("delta").mode("append").saveAsTable("t_builder");
            return null;
          });
    }
    System.out.println("t_builder final:\n" + spark.table("t_builder").schema().treeString());
    exp("t_builder read back (all columns)", () -> spark.table("t_builder"));
    exp("t_builder read back (arr only)", () -> spark.table("t_builder").select("id", "arr"));
    exp(
        "t_builder exists(arr, x -> x is null)",
        () ->
            spark
                .table("t_builder")
                .filter(exists(col("arr"), x -> x.isNull()))
                .select("id", "arr"));

    // b) append nullable data with a null into a NOT NULL table. Make one by DDL.
    spark.sql(
        "CREATE TABLE t_b (id STRING NOT NULL, s STRUCT<x: INT NOT NULL>, arr ARRAY<INT>,"
            + " m MAP<STRING,STRING>) USING delta");
    System.out.println("t_b (DDL) schema:\n" + spark.table("t_b").schema().treeString());
    for (int i = 0; i < BAD_ROWS.size(); i++) {
      final int n = i;
      run(
          "append nullable bad" + n + " into DDL NOT NULL table t_b",
          () -> {
            badNullable(n).write().format("delta").mode("append").saveAsTable("t_b");
            return null;
          });
    }
    run(
        "append nullable good into DDL NOT NULL table t_b",
        () -> {
          spark
              .read()
              .schema(NULLABLE)
              .json(path("good"))
              .write()
              .format("delta")
              .mode("append")
              .saveAsTable("t_b");
          return null;
        });
    System.out.println("t_b after:\n" + spark.table("t_b").schema().treeString());
    System.out.println("t_b rows: " + spark.table("t_b").count());

    // Same, into the path table c (if it kept NOT NULL).
    for (int i = 0; i < BAD_ROWS.size(); i++) {
      final int n = i;
      run(
          "append nullable bad" + n + " into path table c",
          () -> {
            badNullable(n).write().format("delta").mode("append").save(c);
            return null;
          });
    }
    // Delta array/map constraints: DDL table with containsNull=false.
    run(
        "CREATE TABLE with ARRAY<INT NOT NULL>",
        () -> {
          spark.sql("CREATE TABLE t_arr (arr ARRAY<INT NOT NULL>) USING delta");
          System.out.println("t_arr schema:\n" + spark.table("t_arr").schema().treeString());
          return null;
        });
    // Streaming append of nullable data into a nullable table from a NN-marked stream.
    final String sTable = tmp.resolve("d_stream").toString();
    run(
        "streaming append NN-marked stream to new delta path",
        () -> {
          StreamingQuery q =
              Narrow.apply(spark.readStream().schema(NULLABLE).json(path("good")), NN)
                  .writeStream()
                  .format("delta")
                  .option("checkpointLocation", tmp.resolve("ck_ds").toString())
                  .trigger(Trigger.AvailableNow())
                  .start(sTable);
          q.awaitTermination();
          System.out.println("stream table schema:\n" + schemaOf(sTable).treeString());
          return null;
        });
    run(
        "streaming append nullable bad0 into DDL NOT NULL t_b",
        () -> {
          StreamingQuery q =
              spark
                  .readStream()
                  .schema(NULLABLE)
                  .json(path("bad0"))
                  .writeStream()
                  .format("delta")
                  .option("checkpointLocation", tmp.resolve("ck_tb").toString())
                  .trigger(Trigger.AvailableNow())
                  .toTable("t_b");
          q.awaitTermination();
          return null;
        });
  }

  // ---------------------------------------------------------------------------------------------
  // Q5: a false NOT NULL flag gives wrong results.

  static void q5FalseFlag() throws Exception {
    h1("Q5 false NOT NULL flag");
    final String pq = tmp.resolve("pq0").toString();
    if (!Files.exists(tmp.resolve("pq0"))) {
      spark.read().schema(NULLABLE).json(path("bad0")).write().parquet(pq);
    }
    final Dataset<Row> lying = spark.read().schema(NN).parquet(pq);
    System.out.println("parquet schema(NN) id nullable=" + lying.schema().apply("id").nullable());
    exp("parquet NN: all rows", () -> lying);
    exp("parquet NN: filter(id isNull)", () -> lying.filter(col("id").isNull()));
    exp(
        "parquet NN: select id isNull",
        () -> lying.select(col("id"), col("id").isNull().as("isnull")));
    exp("parquet NN: coalesce(id,'X')", () -> lying.select(coalesce(col("id"), lit("X")).as("c")));
    final Dataset<Row> lying2 =
        spark.read().text(path("bad0")).select(from_json(col("value"), NN).as("r")).select("r.*");
    System.out.println("from_json NN id nullable=" + lying2.schema().apply("id").nullable());
    exp("from_json NN: filter(id isNull)", () -> lying2.filter(col("id").isNull()));
  }

  // ---------------------------------------------------------------------------------------------
  // Q6: per-column check is pruned by later queries; a row-level guard filter is not.

  static Dataset<Row> countOf(Dataset<Row> d) {
    return spark.createDataFrame(
        List.of(RowFactory.create(d.count())), new StructType().add("count", DataTypes.LongType));
  }

  static void q6Guard() throws Exception {
    h1("Q6 guard filter vs per-column check");
    for (int i = 0; i < BAD_ROWS.size(); i++) {
      final int n = i;
      exp(
          "Guard+Narrow bad" + n + " count()",
          () -> countOf(Narrow.apply(Guard.apply(badNullable(n), NN), NN)));
    }
    exp(
        "Guard+Narrow bad0 select(s)",
        () -> Narrow.apply(Guard.apply(badNullable(0), NN), NN).select("s"));
    exp(
        "Guard+Narrow bad0 filter(id isNull)",
        () -> Narrow.apply(Guard.apply(badNullable(0), NN), NN).filter(col("id").isNull()));
    exp(
        "Guard+Narrow good",
        () -> Narrow.apply(Guard.apply(spark.read().schema(NULLABLE).json(path("good")), NN), NN));
    exp("Narrow only bad3 count()", () -> countOf(Narrow.apply(badNullable(3), NN)));
    streamExp(
        "stream Guard+Narrow bad1",
        () ->
            Narrow.apply(
                Guard.apply(spark.readStream().schema(NULLABLE).json(path("bad1")), NN), NN));
    System.out.println("--- optimised plan of Guard+Narrow(...).select(s) ---");
    System.out.println(
        Narrow.apply(Guard.apply(badNullable(0), NN), NN)
            .select("s")
            .queryExecution()
            .optimizedPlan()
            .treeString());
  }

  // ---------------------------------------------------------------------------------------------
  // Helpers.

  static void writeJson(String name, List<String> lines) throws Exception {
    Path d = tmp.resolve(name);
    Files.createDirectories(d);
    Files.write(d.resolve("part.json"), lines);
  }

  static String path(String name) {
    return tmp.resolve(name).toString();
  }

  static void h1(String s) {
    System.out.println("\n==================== " + s + " ====================");
  }

  static void exp(String label, Callable<Dataset<Row>> c) {
    System.out.println("\n### " + label);
    final Dataset<Row> df;
    try {
      df = c.call();
    } catch (Throwable t) {
      System.out.println("ERROR AT DEFINITION: " + describe(t));
      return;
    }
    System.out.print(df.schema().treeString());
    try {
      System.out.println("rows: " + Arrays.toString((Object[]) df.collect()));
    } catch (Throwable t) {
      System.out.println("ERROR AT ACTION: " + describe(t));
    }
  }

  static void run(String label, Callable<Object> c) {
    System.out.println("\n### " + label);
    try {
      c.call();
      System.out.println("OK");
    } catch (Throwable t) {
      System.out.println("ERROR: " + describe(t));
    }
  }

  static int streamCounter = 0;

  static void streamExp(String label, Callable<Dataset<Row>> c) {
    System.out.println("\n### " + label);
    final Dataset<Row> df;
    try {
      df = c.call();
    } catch (Throwable t) {
      System.out.println("ERROR AT DEFINITION: " + describe(t));
      return;
    }
    System.out.print(df.schema().treeString());
    final String sink = "mem" + (streamCounter++);
    try {
      final StringBuilder batchSchema = new StringBuilder();
      StreamingQuery q =
          df.writeStream()
              .option("checkpointLocation", tmp.resolve("ck_" + sink).toString())
              .trigger(Trigger.AvailableNow())
              .foreachBatch(
                  (VoidFunction2<Dataset<Row>, Long>)
                      (b, id) -> {
                        batchSchema.append("\n").append(b.schema().treeString());
                        b.write().mode("append").parquet(tmp.resolve("out_" + sink).toString());
                      })
              .start();
      q.awaitTermination();
      System.out.println("foreachBatch batch schema: " + batchSchema);
      System.out.println(
          "rows: "
              + Arrays.toString(
                  (Object[])
                      spark.read().parquet(tmp.resolve("out_" + sink).toString()).collect()));
    } catch (Throwable t) {
      System.out.println("ERROR AT STREAM RUN: " + describe(t));
    }
  }

  static String describe(Throwable t) {
    Throwable root = t;
    while (root.getCause() != null && root.getCause() != root) {
      root = root.getCause();
    }
    String m = String.valueOf(t.getMessage()).replace('\n', ' ');
    String rm = String.valueOf(root.getMessage()).replace('\n', ' ');
    if (m.length() > 400) m = m.substring(0, 400) + "...";
    if (rm.length() > 400) rm = rm.substring(0, 400) + "...";
    return t.getClass().getName()
        + ": "
        + m
        + (root != t ? "\n    ROOT " + root.getClass().getName() + ": " + rm : "");
  }

  static Map<String, Object> unused() {
    return null;
  }
}
