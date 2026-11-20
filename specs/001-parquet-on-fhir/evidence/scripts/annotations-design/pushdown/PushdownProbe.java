import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.encoders.ColumnFunctions;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.search.SearchColumnBuilder;
import java.io.File;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import scala.jdk.javaapi.CollectionConverters;

/**
 * Which expression shapes let Parquet skip row groups for a date-bound filter. Each arm is run as a
 * filter over a Patient table sorted by birthDate and written in many small row groups, with
 * record-level filtering off, and reports the rows the scan emitted against the total.
 *
 * <p>Usage: PushdownProbe [work dir written by PushdownPrepare]
 */
public class PushdownProbe {

  static final Instant TS = Instant.parse("2000-01-01T00:00:00Z");
  static final Instant TS_END = Instant.parse("2000-12-31T23:59:59.999999Z");

  static SparkSession spark;
  static long totalRows;

  static long fsBytesRead() {
    long sum = 0;
    for (final FileSystem.Statistics s : FileSystem.getAllStatistics()) {
      if ("file".equals(s.getScheme())) {
        sum += s.getBytesRead();
      }
    }
    return sum;
  }

  /** Describes the row groups of a table, and how many a bound of a column would leave. */
  static void footer(final String dir, final String column, final Instant bound) throws Exception {
    final Configuration conf = spark.sparkContext().hadoopConfiguration();
    final FileSystem fs = FileSystem.getLocal(conf);
    int groups = 0;
    long rows = 0;
    int expectedGroups = 0;
    long expectedRows = 0;
    boolean stats = true;
    for (final FileStatus f : fs.listStatus(new Path(dir))) {
      if (!f.getPath().getName().endsWith(".parquet")) {
        continue;
      }
      try (final ParquetFileReader r =
          ParquetFileReader.open(HadoopInputFile.fromPath(f.getPath(), conf))) {
        for (final BlockMetaData b : r.getFooter().getBlocks()) {
          groups++;
          rows += b.getRowCount();
          for (final ColumnChunkMetaData c : b.getColumns()) {
            if (c.getPath().toDotString().equals(column)) {
              final Statistics<?> st = c.getStatistics();
              if (st == null || !st.hasNonNullValue()) {
                stats = false;
                expectedGroups++;
                expectedRows += b.getRowCount();
              } else {
                final long maxMicros = ((Number) st.genericGetMax()).longValue();
                if (maxMicros >= bound.getEpochSecond() * 1_000_000L) {
                  expectedGroups++;
                  expectedRows += b.getRowCount();
                }
                if (groups == 1) {
                  System.out.println("    first chunk type=" + c.getPrimitiveType());
                }
              }
            }
          }
        }
      }
    }
    System.out.printf(
        "FOOTER %s: %d row groups, %d rows; column %s has statistics: %s; row groups with max >= %s:"
            + " %d (%d rows)%n",
        new File(dir).getName(), groups, rows, column, stats, bound, expectedGroups, expectedRows);
  }

  static void run(final String label, final Dataset<Row> df, final Column cond) {
    final Dataset<Row> q = df.filter(cond).select("id");
    final long before = fsBytesRead();
    final int matched = q.collectAsList().size();
    final long bytes = fsBytesRead() - before;
    final SparkPlan plan = q.queryExecution().executedPlan();
    long scanned = -1;
    for (final SparkPlan leaf : CollectionConverters.asJava(plan.collectLeaves())) {
      if (leaf.metrics().contains("numOutputRows")) {
        scanned = leaf.metrics().apply("numOutputRows").value();
      }
    }
    final Matcher m = Pattern.compile("PushedFilters: (\\[.*?\\]), ReadSchema").matcher(plan.toString());
    final String pushed = m.find() ? m.group(1) : "n/a";
    final String optimised = q.queryExecution().optimizedPlan().toString().replace('\n', ' ');
    final Matcher fm = Pattern.compile("Filter (.*?) \\+?-?\\s*Relation").matcher(optimised);
    System.out.printf(
        "ARM %-34s matched=%-8d scanned=%-8d of %d  bytesRead=%-10d pushed=%s%n",
        label, matched, scanned, totalRows, bytes, pushed.length() > 160 ? pushed.substring(0, 160) + "..." : pushed);
    if (System.getenv("SHOW_FILTER") != null && fm.find()) {
      System.out.println("      optimised filter: " + fm.group(1));
    }
  }

  public static void main(final String[] args) throws Exception {
    spark =
        SparkSession.builder()
            .master("local[2]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.adaptive.enabled", "false")
            .config("spark.sql.maxMetadataStringLength", "100000")
            .config("spark.sql.parquet.recordLevelFilter.enabled", "false")
            .config("spark.sql.parquet.filterPushdown", "true")
            .getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");
    final PathlingContext ptc = PathlingContext.create(spark);
    final String work = args[0];
    System.out.println("recordLevelFilter=" + spark.conf().get("spark.sql.parquet.recordLevelFilter.enabled")
        + " Spark " + spark.version());

    final Dataset<Row> full = spark.read().parquet(work + "/full");
    totalRows = full.count();
    footer(work + "/full", "_birthDate.__end", TS);
    footer(work + "/full", "_birthDate.__start", TS);

    final Column end = col("_birthDate").getField("__end");
    final Column start = col("_birthDate").getField("__start");
    final Column ts = lit(TS);
    final Column element =
        struct(col("birthDate").alias("value"), col("_birthDate").alias("meta"));

    final String engineFilter =
        full.filter(ptc.fhirPathToColumn("Patient", "birthDate >= @2000"))
            .queryExecution()
            .optimizedPlan()
            .toString();
    System.out.println("ENGINE OPTIMISED PLAN: " + engineFilter.substring(0, Math.min(3000, engineFilter.length())));
    System.out.println("== Arm 1: today, through the engine ==");
    run("engine: birthDate >= @2000", full, ptc.fhirPathToColumn("Patient", "birthDate >= @2000"));
    run(
        "engine: where(birthDate >= @2000).exists()",
        full,
        ptc.fhirPathToColumn("Patient", "where(birthDate >= @2000).exists()"));
    run(
        "engine: birthDate.where($this>=@2000).exists()",
        full,
        ptc.fhirPathToColumn("Patient", "birthDate.where($this >= @2000).exists()"));
    run(
        "engine: name.where(period.start>=@2000)",
        full,
        ptc.fhirPathToColumn("Patient", "name.where(period.start >= @2000).exists()"));
    run(
        "engine: name.period.start >= @2000 (any)",
        full,
        ptc.fhirPathToColumn("Patient", "name.period.start.where($this >= @2000).exists()"));
    run(
        "search: birthdate=ge2000 (DateMatcher)",
        full,
        SearchColumnBuilder.withDefaultRegistry(ptc.getFhirContext())
            .fromQueryString(ResourceType.PATIENT, "birthdate=ge2000"));
    run(
        "search: birthdate=gt2000 (DateMatcher)",
        full,
        SearchColumnBuilder.withDefaultRegistry(ptc.getFhirContext())
            .fromQueryString(ResourceType.PATIENT, "birthdate=gt2000"));

    System.out.println("== Arm 2: plain field ==");
    run("plain: _birthDate.__end >= ts", full, end.geq(ts));
    run("plain: _birthDate.__start >= ts", full, start.geq(ts));
    run(
        "ColumnFunctions.resolveOrElse __end",
        full,
        ColumnFunctions.resolveOrElse(col("_birthDate"), "__end", lit(Instant.EPOCH)).geq(ts));
    run(
        "resolveOrElse, fallback = computed",
        full,
        ColumnFunctions.resolveOrElse(
                col("_birthDate"), "__end", col("birthDate").cast(DataTypes.TimestampType))
            .geq(ts));

    System.out.println("== Arm 3: through the element struct ==");
    run("struct(value, meta).meta.__end >= ts", full, element.getField("meta").getField("__end").geq(ts));

    System.out.println("== Arm 4: through CASE WHEN wrapper ==");
    run(
        "when(notNull, struct).meta.__end >= ts",
        full,
        when(col("birthDate").isNotNull(), element).getField("meta").getField("__end").geq(ts));
    run(
        "when(notNull, struct(resolveOrElse))",
        full,
        when(
                col("birthDate").isNotNull(),
                struct(
                    col("birthDate").alias("value"),
                    ColumnFunctions.resolveOrElse(col("_birthDate"), "__end", lit(Instant.EPOCH))
                        .alias("__end")))
            .getField("__end")
            .geq(ts));

    System.out.println("== Arm 5: comparison that is empty where it cannot compare ==");
    final Column canCompare =
        end.lt(lit(TS))
            .or(lit(TS_END).lt(start))
            .or(lit(TS).equalTo(start).and(end.equalTo(lit(TS_END))));
    final Column cmp = when(canCompare, start.geq(ts)).otherwise(lit(null));
    run("CASE canCompare THEN start>=ts ELSE null", full, cmp);
    run("  ... AND __end >= ts", full, cmp.and(end.geq(ts)));
    run("  ... AND __start >= ts", full, cmp.and(start.geq(ts)));
    run("  __end>=ts AND CASE", full, end.geq(ts).and(cmp));
    run("  coalesce(CASE, false)", full, coalesce(cmp, lit(false)));
    run("CASE end notNull THEN end>=ts", full, when(end.isNotNull(), end.geq(ts)));
    run("  NOT NOT CASE (not simplified?)", full, cmp.isNotNull().and(cmp));
    run("  CASE as isNotNull(CASE) (exists)", full, when(canCompare, start.geq(ts)).isNotNull());

    System.out.println("== Extra: the Period shape of DateMatcher, a bound taken as infinity ==");
    run("coalesce(__end, MAX) >= ts", full, coalesce(end, lit(Instant.parse("9999-12-31T23:59:59Z"))).geq(ts));
    run("__end >= ts OR __end IS NULL", full, end.geq(ts).or(end.isNull()));
    run("CASE canCompare THEN start>=ts", full, when(canCompare, start.geq(ts)));

    System.out.println("== Arms 6 and 7: storage type, grouped vs flat ==");
    for (final String name : new String[] {"narrow_micros", "narrow_int96", "narrow_grouped"}) {
      final Dataset<Row> df = spark.read().parquet(work + "/" + name);
      totalRows = df.count();
      final boolean grouped = name.endsWith("grouped");
      final String path = grouped ? "_birthDate.__bounds.upper" : "_birthDate.__end";
      footer(work + "/" + name, path, TS);
      run(name + " " + path + " >= ts", df, col(path).geq(ts));
      if (grouped) {
        run(name + " " + "_birthDate.__bounds.lower >= ts", df, col("_birthDate.__bounds.lower").geq(ts));
      }
    }
    spark.stop();
  }
}
