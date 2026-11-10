/**
 * Measures what unused element-level metadata costs when the source is in memory. Usage:
 * CachedSourceBench source variant arm exprs, where source is cache (Parquet then cache) or json
 * (NDJSON through the library, then cache), variant is plain or ext, arm is bare, A0, A1, L1 or
 * D3a, and exprs is E1,E2 (Patient) or E3 (Encounter). Data directories come from the system
 * properties pq (the Parquet written by Prep) and nd (the NDJSON).
 *
 * <p>Arms: A0 is the engine through a ViewDefinition over the cached dataset. A1 pairs each value
 * with its whole {@code _x} group at traversal. L1 shapes the element in the planner and carries
 * only the named annotation fields. D3a (E3 only) takes leaves with extraction chains outside any
 * lambda and re-zips. bare reads the values and nothing else.
 */
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.storage.RDDInfo;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class CachedSourceBench {

  static final String LIT_S = "timestamp'2020-01-01 00:00:00'";
  static final String LIT_E = "timestamp'2020-01-01 23:59:59.999'";

  /** The engine's comparison of a date range against the literal range of @2020-01-01, for >. */
  static String cmp(final String s, final String e) {
    return "CASE WHEN ((" + e + " < " + LIT_S + ") OR (" + LIT_E + " < " + s + ")) OR (("
        + LIT_S + " = " + s + ") AND (" + e + " = " + LIT_E + ")) THEN (" + s + " > " + LIT_S
        + ") END";
  }

  static Column x(final String sql) {
    return functions.expr(sql);
  }

  static Column column(final String arm, final String expr) {
    switch (expr + "/" + arm) {
      case "E1/bare":
        return x("birthDate");
      case "E1/A1":
        return x("element_at(transform(filter(array(CASE WHEN birthDate IS NOT NULL THEN"
            + " named_struct('value', birthDate, 'meta', _birthDate) END), v -> v IS NOT NULL),"
            + " v -> v.value), 1)");
      case "E1/L1":
        return x("element_at(transform(filter(array(CASE WHEN birthDate IS NOT NULL THEN"
            + " named_struct('value', birthDate) END), v -> v IS NOT NULL), v -> v.value), 1)");
      case "E2/bare":
        return x("transform(name, n -> n.given)");
      case "E2/A1":
        return x("flatten(transform(filter(name, n -> n.given IS NOT NULL), n ->"
            + " arrays_zip(n.given, coalesce(n._given, array()))))").getField("given");
      case "E2/L1":
        return x("flatten(transform(filter(name, n -> n.given IS NOT NULL), n -> n.given))");
      case "E3/bare":
        return x("transform(filter(location, l -> l.period._start.__start > " + LIT_E
            + "), l -> l.period.start)");
      case "E3/L1":
        return x("transform(filter(filter(transform(filter(filter(location, l -> l IS NOT NULL)"
            + ".period, p -> p IS NOT NULL), p -> CASE WHEN p.start IS NOT NULL THEN"
            + " named_struct('value', p.start, '__start', p._start.__start, '__end',"
            + " p._start.__end) END), e -> e IS NOT NULL), e -> "
            + cmp("e.__start", "e.__end") + "), e -> e.value)");
      case "E3/D3a":
        return x("filter(transform(filter(arrays_zip(location.period.start,"
            + " location.period._start.__start, location.period._start.__end), z -> "
            + cmp("z.__start", "z.__end") + "), z -> z.start), v -> v IS NOT NULL)");
      default:
        throw new IllegalArgumentException(expr + "/" + arm);
    }
  }

  static String fhirPath(final String expr) {
    switch (expr) {
      case "E1":
        return "birthDate";
      case "E2":
        return "name.given";
      default:
        return "location.period.where(start > @2020-01-01).start";
    }
  }

  static String view(final String resource, final String path, final boolean collection) {
    return "{\"resourceType\":\"ViewDefinition\",\"resource\":\"" + resource
        + "\",\"status\":\"active\",\"select\":[{\"column\":[{\"path\":\"" + path
        + "\",\"name\":\"r\",\"collection\":" + collection + "}]}]}";
  }

  static long gcMillis() {
    return ManagementFactory.getGarbageCollectorMXBeans().stream()
        .mapToLong(GarbageCollectorMXBean::getCollectionTime)
        .sum();
  }

  public static void main(final String[] args) {
    final String source = args[0];
    final String variant = args[1];
    final String arm = args[2];
    final List<String> exprs = Arrays.asList(args[3].split(","));
    final boolean patient = !exprs.contains("E3");
    final String type = patient ? "Patient" : "Encounter";
    final SparkSession spark =
        SparkSession.builder()
            .master("local[4]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.maxMetadataStringLength", "100000")
            .config("spark.sql.adaptive.enabled", "false")
            .getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");
    final PathlingContext ptc = PathlingContext.create(spark);

    Dataset<Row> raw;
    if (source.equals("cache")) {
      raw =
          spark
              .read()
              .parquet(System.getProperty("pq") + "/" + variant + "/" + type + ".parquet")
              .repartition(4);
    } else {
      raw = Splice.apply(
          ptc.read().ndjson(System.getProperty("nd")).read(type), type, variant.equals("ext"));
    }
    final Dataset<Row> cached = raw.cache();
    final long b0 = System.nanoTime();
    cached.write().format("noop").mode("overwrite").save();
    final double buildSecs = (System.nanoTime() - b0) / 1e9;
    final long rows = cached.count();
    for (final RDDInfo info : spark.sparkContext().getRDDStorageInfo()) {
      System.out.printf(
          "CACHE source=%s variant=%s table=%s rows=%d mem_mb=%.0f disk_mb=%.0f cached_partitions=%d"
              + " of %d build_secs=%.1f%n",
          source, variant, type, rows, info.memSize() / 1048576.0, info.diskSize() / 1048576.0,
          info.numCachedPartitions(), info.numPartitions(), buildSecs);
    }
    final QueryableDataSource engine = ptc.read().datasets().dataset(type, cached);

    for (final String e : exprs) {
      final Dataset<Row> df =
          arm.equals("A0")
              ? engine.view(type).json(view(type, fhirPath(e), !e.equals("E1"))).execute()
              : cached.select(column(arm, e).alias("r"));
      final String plan = df.queryExecution().executedPlan().toString();
      final String scan =
          Arrays.stream(plan.split("\n"))
              .filter(l -> l.contains("InMemoryTableScan"))
              .map(String::trim)
              .collect(Collectors.joining(" | "));
      final int rel = plan.indexOf("InMemoryRelation");
      final String head = rel > 0 ? plan.substring(0, rel) : plan;
      System.out.println("PLAN " + arm + " " + e + " " + head.replace("\n", " ~ "));
      final Row check =
          df.agg(
                  e.equals("E1")
                      ? functions.count(functions.col("r"))
                      : functions.sum(functions.coalesce(functions.size(functions.col("r")),
                          functions.lit(0))))
              .first();
      final List<Double> times = new ArrayList<>();
      long gcTotal = 0;
      for (int i = 0; i < 6; i++) {
        System.gc();
        final long gc = gcMillis();
        final long t0 = System.nanoTime();
        df.write().format("noop").mode("overwrite").save();
        final double secs = (System.nanoTime() - t0) / 1e9;
        if (i > 0) {
          times.add(secs);
          gcTotal += gcMillis() - gc;
        }
      }
      Collections.sort(times);
      System.out.printf(
          "RESULT source=%s variant=%s arm=%s expr=%s median=%.2f min=%.2f max=%.2f gc_ms_total=%d"
              + " out=%s date_range_in_plan=%s scan=%s%n",
          source, variant, arm, e, times.get(2), times.get(0), times.get(4), gcTotal,
          check.get(0), head.contains("date_range"), scan);
    }
    spark.stop();
  }
}
