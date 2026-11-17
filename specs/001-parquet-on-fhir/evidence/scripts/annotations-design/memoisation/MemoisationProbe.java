import au.csiro.pathling.schema.date.FhirDateTime;
import au.csiro.pathling.sql.SqlFunctions;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * Compares four ways of computing the two bounds of a date from its text, in several plan contexts:
 * the struct UDF bound once, the struct UDF referenced twice, two scalar UDFs, and two scalar UDFs
 * over a per-thread LRU of parses. Counts the real parses, and times each.
 *
 * <p>Usage: MemoisationProbe count|time|trace dataDir wholeStage|nowholestage|interpreted len [rows]
 * [cacheSize] [contexts,comma-separated]. The master is -Dmaster (default local[4]).
 */
public class MemoisationProbe {

  static final LongAdder PARSES = new LongAdder();
  static final LongAdder UDF_CALLS = new LongAdder();
  static final LongAdder TRACES = new LongAdder();
  static int CACHE_SIZE = 32;

  static final StructType RANGE =
      new StructType().add("s", DataTypes.TimestampType).add("e", DataTypes.TimestampType);

  static FhirDateTime parse(final String text) {
    PARSES.increment();
    return FhirDateTime.parse(text);
  }

  static final ThreadLocal<LinkedHashMap<String, FhirDateTime>> CACHE =
      ThreadLocal.withInitial(
          () ->
              new LinkedHashMap<>(64, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(final Map.Entry<String, FhirDateTime> e) {
                  return size() > CACHE_SIZE;
                }
              });

  static FhirDateTime cached(final String text) {
    final LinkedHashMap<String, FhirDateTime> c = CACHE.get();
    FhirDateTime v = c.get(text);
    if (v == null) {
      v = parse(text);
      c.put(text, v);
    }
    return v;
  }

  interface SerFn extends java.io.Serializable {
    Instant apply(String t);
  }

  static UserDefinedFunction scalar(
      final String name, final SerFn f) {
    return functions
        .udf(
            (UDF1<String, Instant>)
                t -> {
                  if (t == null) {
                    return null;
                  }
                  UDF_CALLS.increment();
                  return f.apply(t);
                },
            DataTypes.TimestampType)
        .withName(name);
  }

  static final UserDefinedFunction RANGE_UDF =
      functions
          .udf(
              (UDF1<String, Row>)
                  t -> {
                    if (t == null) {
                      return null;
                    }
                    UDF_CALLS.increment();
                    final FhirDateTime p = parse(t);
                    return RowFactory.create(p.getLowerBoundary(), p.getUpperBoundary());
                  },
              RANGE)
          .withName("date_range");

  static final UserDefinedFunction START_PLAIN =
      scalar("date_start", t -> parse(t).getLowerBoundary());
  static final UserDefinedFunction END_PLAIN = scalar("date_end", t -> parse(t).getUpperBoundary());
  static final UserDefinedFunction START_CACHED =
      scalar("date_start_c", t -> cached(t).getLowerBoundary());
  static final UserDefinedFunction END_CACHED =
      scalar("date_end_c", t -> cached(t).getUpperBoundary());

  static final UserDefinedFunction TRACE =
      functions
          .udf(
              (UDF1<String, String>)
                  t -> {
                    TRACES.increment();
                    return t;
                  },
              DataTypes.StringType)
          .asNondeterministic()
          .withName("trace");

  static final String[] ARMS = {
    "1-struct+bindOnce", "2-struct-twice-CSE", "3-scalar-nocache", "4-scalar-LRU"
  };
  static final String[] CONTEXTS = {"proj", "proj2col", "filter", "lambda", "passes", "arrfilter"};

  static final Column X = functions.lit("1990-06-01 00:00:00").cast("timestamp");

  interface Comb {
    Column apply(Column s, Column e);
  }

  static Column contains(final Column s, final Column e) {
    return s.leq(X).and(e.geq(X));
  }

  static Column both(final Column s, final Column e) {
    return functions.struct(s.as("s"), e.as("e"));
  }

  static Column bindOnce(final Column v, final UnaryOperator<Column> body) {
    return functions.element_at(functions.transform(functions.array(v), body::apply), 1);
  }

  /** Combines both bounds of a text in one expression, as the arm dictates. */
  static Column pair(final int arm, final Column t, final Comb comb) {
    switch (arm) {
      case 0:
        return bindOnce(RANGE_UDF.apply(t), r -> comb.apply(r.getField("s"), r.getField("e")));
      case 1:
        return comb.apply(RANGE_UDF.apply(t).getField("s"), RANGE_UDF.apply(t).getField("e"));
      case 2:
        return comb.apply(START_PLAIN.apply(t), END_PLAIN.apply(t));
      default:
        return comb.apply(START_CACHED.apply(t), END_CACHED.apply(t));
    }
  }

  static Dataset<Row> plan(
      final Dataset<Row> df, final String ctx, final int arm, final UnaryOperator<Column> wrap) {
    final Column text = df.col("t");
    final Column arr = df.col("a");
    switch (ctx) {
      case "proj":
        return df.select(pair(arm, wrap.apply(text), MemoisationProbe::both).as("r"));
      case "filter":
        return df.filter(pair(arm, wrap.apply(text), MemoisationProbe::contains));
      case "lambda":
        return df.select(
            functions
                .transform(arr, x -> pair(arm, wrap.apply(x), MemoisationProbe::both))
                .as("r"));
      case "arrfilter":
        return df.select(
            functions
                .filter(arr, x -> pair(arm, wrap.apply(x), MemoisationProbe::contains))
                .as("r"));
      case "proj2col":
        {
          final Column t = wrap.apply(text);
          if (arm == 0) {
            // Two output columns cannot share one bindOnce, so each binds its own.
            return df.select(
                bindOnce(RANGE_UDF.apply(t), r -> r.getField("s")).as("s"),
                bindOnce(RANGE_UDF.apply(t), r -> r.getField("e")).as("e"));
          }
          return df.select(pair(arm, t, (s, e) -> s).as("s"), pair(arm, t, (s, e) -> e).as("e"));
        }
      case "passes":
        {
          final UnaryOperator<Column> sOf;
          final UnaryOperator<Column> eOf;
          switch (arm) {
            case 0:
            case 1:
              sOf = x -> RANGE_UDF.apply(wrap.apply(x)).getField("s");
              eOf = x -> RANGE_UDF.apply(wrap.apply(x)).getField("e");
              break;
            case 2:
              sOf = x -> START_PLAIN.apply(wrap.apply(x));
              eOf = x -> END_PLAIN.apply(wrap.apply(x));
              break;
            default:
              sOf = x -> START_CACHED.apply(wrap.apply(x));
              eOf = x -> END_CACHED.apply(wrap.apply(x));
          }
          return df.select(
              functions.transform(arr, sOf::apply).as("s"),
              functions.transform(arr, eOf::apply).as("e"));
        }
      default:
        throw new IllegalArgumentException(ctx);
    }
  }

  /** The traced variants: the input is bound by a let, as the design says. */
  static Dataset<Row> planLet(final Dataset<Row> df, final String ctx, final int arm) {
    final Column text = df.col("t");
    final Column arr = df.col("a");
    final UnaryOperator<Column> body = v -> pair(arm, v, MemoisationProbe::both);
    if (ctx.equals("proj")) {
      return df.select(SqlFunctions.let(TRACE.apply(text), body).as("r"));
    }
    return df.select(
        functions.transform(arr, x -> SqlFunctions.let(TRACE.apply(x), body)).as("r"));
  }

  static void run(final Dataset<Row> d) {
    d.write().format("noop").mode("overwrite").save();
  }

  /** A checksum of the result, to verify that every arm and mode gives the same answer. */
  static String checksum(final Dataset<Row> d, final String ctx) {
    final Dataset<Row> c;
    switch (ctx) {
      case "filter":
        return "n=" + d.count();
      case "proj":
        c =
            d.selectExpr(
                "sum(cast(unix_micros(r.s) as decimal(38,0)))", "sum(cast(unix_micros(r.e) as decimal(38,0)))", "count(*)");
        break;
      case "proj2col":
        c = d.selectExpr("sum(cast(unix_micros(s) as decimal(38,0)))", "sum(cast(unix_micros(e) as decimal(38,0)))", "count(*)");
        break;
      case "passes":
        c =
            d.selectExpr(
                "sum(aggregate(s, cast(0 as decimal(38,0)), (acc,x) -> acc + cast(unix_micros(x) as decimal(38,0))))",
                "sum(aggregate(e, cast(0 as decimal(38,0)), (acc,x) -> acc + cast(unix_micros(x) as decimal(38,0))))",
                "count(*)");
        break;
      case "lambda":
        c =
            d.selectExpr(
                "sum(aggregate(r, cast(0 as decimal(38,0)), (acc,x) -> acc + cast(unix_micros(x.s) as decimal(38,0))))",
                "sum(aggregate(r, cast(0 as decimal(38,0)), (acc,x) -> acc + cast(unix_micros(x.e) as decimal(38,0))))",
                "count(*)");
        break;
      default:
        c = d.selectExpr("sum(size(r))", "count(*)");
    }
    return c.collectAsList().get(0).mkString("[", ",", "]");
  }

  static String sample(final Random r) {
    final int y = 1950 + r.nextInt(75);
    final int m = 1 + r.nextInt(12);
    final int d = 1 + r.nextInt(28);
    final int roll = r.nextInt(10);
    if (roll < 2) {
      return String.format("%04d", y);
    }
    if (roll < 4) {
      return String.format("%04d-%02d", y, m);
    }
    if (roll < 7) {
      return String.format("%04d-%02d-%02d", y, m, d);
    }
    final String tm =
        String.format(
            "%04d-%02d-%02dT%02d:%02d:%02d", y, m, d, r.nextInt(24), r.nextInt(60), r.nextInt(60));
    switch (r.nextInt(3)) {
      case 0:
        return tm + "Z";
      case 1:
        return tm + ".123+10:00";
      default:
        return tm + "-05:00";
    }
  }

  public static void main(final String[] args) {
    final String mode = args[0];
    final String dir = args[1];
    final String cg = args[2];
    final int len = Integer.parseInt(args[3]);
    final int rows = args.length > 4 ? Integer.parseInt(args[4]) : 1_000_000;
    if (args.length > 5) {
      CACHE_SIZE = Integer.parseInt(args[5]);
    }
    final String[] ctxs = args.length > 6 ? args[6].split(",") : CONTEXTS;

    final SparkSession.Builder b =
        SparkSession.builder()
            .master(System.getProperty("master", "local[4]"))
            .appName("memo")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.adaptive.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC");
    if (cg.equals("nowholestage")) {
      b.config("spark.sql.codegen.wholeStage", "false");
    }
    if (cg.equals("interpreted")) {
      b.config("spark.sql.codegen.wholeStage", "false")
          .config("spark.sql.codegen.factoryMode", "NO_CODEGEN");
    }
    final SparkSession spark = b.getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");

    final String path = dir + "/data-" + rows + "x" + len;
    if (!new java.io.File(path).exists()) {
      final Random r = new Random(42);
      final String[] pool = new String[5000];
      for (int i = 0; i < pool.length; i++) {
        pool[i] = sample(r);
      }
      final List<Row> list = new ArrayList<>(rows);
      for (int i = 0; i < rows; i++) {
        final String[] a = new String[len];
        for (int j = 0; j < len; j++) {
          a[j] = pool[r.nextInt(pool.length)];
        }
        list.add(RowFactory.create(a[0], a));
      }
      final StructType st =
          new StructType()
              .add("t", DataTypes.StringType)
              .add("a", DataTypes.createArrayType(DataTypes.StringType));
      spark.createDataFrame(list, st).repartition(8).write().parquet(path);
    }
    final Dataset<Row> df = spark.read().parquet(path);
    final long elems = (long) rows * len;

    for (final String ctx : ctxs) {
      final boolean arrCtx =
          !ctx.equals("proj") && !ctx.equals("proj2col") && !ctx.equals("filter");
      final long values = arrCtx ? elems : rows;
      if (mode.equals("trace")) {
        if (!ctx.equals("proj") && !ctx.equals("lambda")) {
          continue;
        }
        for (int arm = 0; arm < 4; arm++) {
          for (final String variant : new String[] {"naive", "let"}) {
            PARSES.reset();
            TRACES.reset();
            UDF_CALLS.reset();
            final Dataset<Row> d;
            if (variant.equals("naive")) {
              d = plan(df, ctx, arm, TRACE::apply);
            } else if (arm == 0) {
              final UnaryOperator<Column> bo =
                  x -> bindOnce(RANGE_UDF.apply(TRACE.apply(x)), r -> both(r.getField("s"), r.getField("e")));
              d =
                  df.select(
                      (ctx.equals("proj")
                              ? bo.apply(df.col("t"))
                              : functions.transform(df.col("a"), bo::apply))
                          .as("r"));
            } else {
              d = planLet(df, ctx, arm);
            }
            run(d);
            System.out.printf(
                "RESULT trace ctx=%s arm=%s variant=%s cg=%s values=%d traces/value=%.3f parses/value=%.3f udfcalls/value=%.3f%n",
                ctx, ARMS[arm], variant, cg, values,
                TRACES.sum() / (double) values,
                PARSES.sum() / (double) values,
                UDF_CALLS.sum() / (double) values);
          }
        }
        continue;
      }
      for (int arm = 0; arm < 4; arm++) {
        final Dataset<Row> d = plan(df, ctx, arm, c -> c);
        if (mode.equals("count")) {
          PARSES.reset();
          UDF_CALLS.reset();
          run(d);
          final long p = PARSES.sum();
          final long u = UDF_CALLS.sum();
          final String cs = checksum(d, ctx);
          System.out.printf(
              "RESULT count ctx=%s arm=%s cg=%s len=%d cache=%d values=%d parses=%d parses/value=%.3f udfcalls/value=%.3f check=%s%n",
              ctx, ARMS[arm], cg, len, CACHE_SIZE, values, p, p / (double) values,
              u / (double) values, cs);
        } else {
          run(d);
          final double[] t = new double[5];
          for (int i = 0; i < 5; i++) {
            final long t0 = System.nanoTime();
            run(d);
            t[i] = (System.nanoTime() - t0) / 1e6;
          }
          Arrays.sort(t);
          System.out.printf(
              "RESULT time ctx=%s arm=%s cg=%s len=%d values=%d median_ms=%.0f min=%.0f max=%.0f%n",
              ctx, ARMS[arm], cg, len, values, t[2], t[0], t[4]);
        }
      }
    }
    spark.stop();
  }
}
