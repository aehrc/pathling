/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import au.csiro.pathling.schema.LayoutFields;
import au.csiro.pathling.schema.annotation.PerThreadLruCache;
import au.csiro.pathling.schema.annotation.kinds.CanonicalQuantity;
import au.csiro.pathling.schema.annotation.kinds.QuantityCanonicalKind;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.api.java.UDF4;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

/**
 * Times the parts of computing a quantity's two canonical annotations. Each variant runs in a JVM
 * of its own (-Donly=variant), over the Observations EngineAnnotationProbe writes.
 *
 * <p>The {@code both}, {@code code-only}, {@code both-narrow} and {@code code+*} variants call the
 * kind itself, so what they measure depends on the build: from {@code 5fec8fb57d} the kind's
 * two-argument {@code compute} over a structure is the tolerant structure path, which the engine
 * and the annotation step no longer take, so these variants do not reproduce the tables measured at
 * {@code 7d22534b9f} and {@code 2d9da8b3e0}. The replica variants are self-contained.
 *
 * <p>The replica variants run the kind's memoised canonicalisation, with the same cache and the
 * same last-input check, in functions of this probe's own, so that the form of the input is the
 * only thing that differs between them.
 */
public final class QuantityCostProbe {

  /**
   * Reads a text field of a structure by name, null where it is absent, as the kind read its input
   * before it took the fields as columns ({@code RowFields.requireString}, since removed).
   */
  static String requireString(final Row row, final String name) {
    final scala.Option<Object> found = row.schema().getFieldIndex(name);
    if (found.isEmpty()) {
      return null;
    }
    final int index = (Integer) found.get();
    if (!(row.schema().fields()[index].dataType() instanceof org.apache.spark.sql.types.StringType)) {
      throw new IllegalArgumentException("Field '" + name + "' must be a string");
    }
    return row.isNullAt(index) ? null : row.getString(index);
  }

  /** The memoised canonicalisation, as the kind holds it, for the replica variants. */
  static final PerThreadLruCache<List<String>, Optional<CanonicalQuantity>> CACHE =
      new PerThreadLruCache<>(
          64,
          key ->
              Optional.ofNullable(
                  QuantityCanonicalKind.toExact(
                      QuantityCanonicalKind.canonicalOf(
                          QuantityCanonicalKind.parseValue(key.get(0)), key.get(1), key.get(2)))));

  static Optional<CanonicalQuantity> exact(final String value, final String system, final String code) {
    return value == null ? Optional.empty() : CACHE.get(Arrays.asList(value, system, code));
  }

  /** The replica over the whole structure, read by name as the kind reads it. */
  static final UserDefinedFunction ROW_CODE =
      functions.udf((UDF1<Row, String>) r -> r == null ? null
          : exact(requireString(r, "value"),
                  requireString(r, "system"),
                  requireString(r, "code"))
              .map(CanonicalQuantity::getCode).orElse(null),
          DataTypes.StringType).withName("row_code");
  static final UserDefinedFunction ROW_VALUE =
      functions.udf((UDF1<Row, Row>) r -> r == null ? null
          : QuantityCanonicalKind.toValueExactRow(
              exact(requireString(r, "value"),
                    requireString(r, "system"),
                    requireString(r, "code")).orElse(null)),
          QuantityCanonicalKind.EXACT_VALUE_TYPE).withName("row_value");

  /** Not a valid layout: the second annotation as the unscaled value alone, with no structure. */
  static final UserDefinedFunction ROW_UNSCALED =
      functions.udf((UDF1<Row, java.math.BigDecimal>) r -> {
            if (r == null) return null;
            final Row e = QuantityCanonicalKind.toValueExactRow(
                exact(requireString(r, "value"),
                      requireString(r, "system"),
                      requireString(r, "code")).orElse(null));
            return e == null ? null : (java.math.BigDecimal) e.get(0);
          },
          DataTypes.createDecimalType(38, 0)).withName("row_unscaled");

  static final Row CONSTANT_VALUE =
      org.apache.spark.sql.RowFactory.create(new java.math.BigDecimal("72500"), 0);

  /** The input converted, and a constant of each annotation's type returned, with no cache. */
  static final UserDefinedFunction CONST_CODE =
      functions.udf((UDF1<Row, String>) r -> r == null ? null : "g", DataTypes.StringType)
          .withName("const_code");
  static final UserDefinedFunction CONST_VALUE =
      functions.udf((UDF1<Row, Row>) r -> r == null ? null : CONSTANT_VALUE,
          QuantityCanonicalKind.EXACT_VALUE_TYPE).withName("const_value");
  static final UserDefinedFunction CONV_NULL =
      functions.udf((UDF1<Row, Row>) r -> null, QuantityCanonicalKind.EXACT_VALUE_TYPE)
          .withName("conv_null");

  static final UserDefinedFunction CONST_UNSCALED =
      functions.udf((UDF1<Row, java.math.BigDecimal>) r -> r == null ? null
              : (java.math.BigDecimal) CONSTANT_VALUE.get(0),
          DataTypes.createDecimalType(38, 0)).withName("const_unscaled");
  static final UserDefinedFunction CONST_CODE2 =
      functions.udf((UDF1<Row, String>) r -> r == null ? null : "g", DataTypes.StringType)
          .withName("const_code2");

  /** The replica over the three fields as separate arguments. */
  static final UserDefinedFunction ARG_CODE =
      functions.udf((UDF3<String, String, String, String>) (v, sy, c) ->
          exact(v, sy, c).map(CanonicalQuantity::getCode).orElse(null),
          DataTypes.StringType).withName("arg_code");
  static final UserDefinedFunction ARG_VALUE =
      functions.udf((UDF3<String, String, String, Row>) (v, sy, c) ->
          QuantityCanonicalKind.toValueExactRow(exact(v, sy, c).orElse(null)),
          QuantityCanonicalKind.EXACT_VALUE_TYPE).withName("arg_value");

  /**
   * A bound, not a design: the second call is keyed by a hash Spark computes from the undecoded
   * strings, and takes no strings at all. A collision would give a wrong answer, so this only
   * shows what not decoding the strings for the second call would save.
   */
  static final PerThreadLruCache<Long, Optional<CanonicalQuantity>> HASHED =
      new PerThreadLruCache<>(64, key -> Optional.empty());
  static final ThreadLocal<long[]> LAST_HASH = ThreadLocal.withInitial(() -> new long[1]);
  static final ThreadLocal<Object[]> LAST_EXACT = ThreadLocal.withInitial(() -> new Object[1]);
  static final UserDefinedFunction HASH_CODE =
      functions.udf((UDF4<Long, String, String, String, String>) (h, v, sy, c) -> {
            final Optional<CanonicalQuantity> e = exact(v, sy, c);
            LAST_HASH.get()[0] = h;
            LAST_EXACT.get()[0] = e;
            return e.map(CanonicalQuantity::getCode).orElse(null);
          },
          DataTypes.StringType).withName("hash_code");
  @SuppressWarnings("unchecked")
  static final UserDefinedFunction HASH_VALUE =
      functions.udf((UDF1<Long, Row>) h -> h != null && LAST_HASH.get()[0] == h
              ? QuantityCanonicalKind.toValueExactRow(
                  ((Optional<CanonicalQuantity>) LAST_EXACT.get()[0]).orElse(null))
              : null,
          QuantityCanonicalKind.EXACT_VALUE_TYPE).withName("hash_value");

  static final UserDefinedFunction NOOP =
      functions.udf((UDF1<Row, String>) r -> null, DataTypes.StringType).withName("noop_conv");
  static final UserDefinedFunction KEY =
      functions.udf(
          (UDF1<Row, String>) r -> {
            if (r == null) return null;
            final int h =
                Arrays.asList(
                        (String) r.getAs("value"), (String) r.getAs("system"), (String) r.getAs("code"))
                    .hashCode();
            return h == 42 ? "x" : null;
          },
          DataTypes.StringType).withName("noop_key");

  static final org.apache.spark.sql.types.StructType OLD_TYPE =
      new org.apache.spark.sql.types.StructType()
          .add("code", DataTypes.StringType)
          .add("value", QuantityCanonicalKind.EXACT_VALUE_TYPE);
  static final UserDefinedFunction OLD =
      functions.udf(
          (UDF1<Row, Row>) r -> {
            if (r == null) return null;
            final au.csiro.pathling.schema.annotation.kinds.CanonicalQuantity c =
                QuantityCanonicalKind.toExact(
                    QuantityCanonicalKind.canonicalOf(
                        QuantityCanonicalKind.parseValue((String) r.getAs("value")),
                        (String) r.getAs("system"),
                        (String) r.getAs("code")));
            return c == null ? null : org.apache.spark.sql.RowFactory.create(c.getCode(),
                QuantityCanonicalKind.toValueExactRow(c));
          },
          OLD_TYPE).withName("old_struct");

  public static void main(final String[] args) {
    final SparkSession spark = SparkSession.builder().master("local[4]")
        .config("spark.sql.session.timeZone", "UTC").config("spark.sql.adaptive.enabled", "false")
        .config("spark.ui.enabled", "false").getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");
    final Dataset<Row> obs = spark.read().parquet(args[0]);
    final int runs = Integer.parseInt(args[1]);
    final QuantityCanonicalKind k = QuantityCanonicalKind.INSTANCE;
    final String C = LayoutFields.CANONICAL_CODE_ANNOTATION;
    final String V = LayoutFields.CANONICAL_VALUE_EXACT_ANNOTATION;
    final Column q = functions.col("valueQuantity");
    final Column narrow = functions.struct(q.getField("value").alias("value"),
        q.getField("system").alias("system"), q.getField("code").alias("code"));
    System.out.println("SCHEMA " + obs.schema().apply("valueQuantity").dataType().simpleString());
    final Map<String, Supplier<Column[]>> v = new LinkedHashMap<>();
    v.put("old-struct", () -> new Column[] {OLD.apply(q)});
    final Column fv = q.getField("value");
    final Column fs = q.getField("system");
    final Column fc = q.getField("code");
    final Column hash = functions.xxhash64(fv, fs, fc);
    v.put("replica-row-both", () -> new Column[] {ROW_CODE.apply(q), ROW_VALUE.apply(q).alias("b")});
    v.put("replica-row-both-decimal",
        () -> new Column[] {ROW_CODE.apply(q), ROW_UNSCALED.apply(q).alias("b")});
    v.put("replica-row-code+conv",
        () -> new Column[] {ROW_CODE.apply(q), CONV_NULL.apply(q).alias("b")});
    v.put("replica-row-code+const-value",
        () -> new Column[] {ROW_CODE.apply(q), CONST_VALUE.apply(q).alias("b")});
    v.put("const-both", () -> new Column[] {CONST_CODE.apply(q), CONST_VALUE.apply(q).alias("b")});
    v.put("const-code", () -> new Column[] {CONST_CODE.apply(q)});
    v.put("const-code+conv", () -> new Column[] {CONST_CODE.apply(q), CONV_NULL.apply(q).alias("b")});
    v.put("const-code+string",
        () -> new Column[] {CONST_CODE.apply(q), CONST_CODE2.apply(q).alias("b")});
    v.put("const-code+decimal",
        () -> new Column[] {CONST_CODE.apply(q), CONST_UNSCALED.apply(q).alias("b")});
    v.put("replica-row-code+cache",
        () -> new Column[] {ROW_CODE.apply(q), ROW_CODE.apply(functions.struct(
            q.getField("value").alias("value"), q.getField("unit").alias("unit"),
            q.getField("system").alias("system"), q.getField("code").alias("code"))).alias("b")});
    v.put("replica-row-code", () -> new Column[] {ROW_CODE.apply(q)});
    v.put("replica-args-both",
        () -> new Column[] {ARG_CODE.apply(fv, fs, fc), ARG_VALUE.apply(fv, fs, fc).alias("b")});
    v.put("replica-args-code", () -> new Column[] {ARG_CODE.apply(fv, fs, fc)});
    v.put("bound-hash-both",
        () -> new Column[] {HASH_CODE.apply(hash, fv, fs, fc), HASH_VALUE.apply(hash).alias("b")});
    v.put("code+conv", () -> new Column[] {k.compute(C, q), NOOP.apply(q).alias("b")});
    v.put("code+key", () -> new Column[] {k.compute(C, q), KEY.apply(q).alias("b")});
    v.put("code+code", () -> new Column[] {k.compute(C, q), k.compute(C, narrow).alias("b")});
    v.put("read", () -> new Column[] {q.getField("code")});
    v.put("noop-conv-x1", () -> new Column[] {NOOP.apply(q)});
    v.put("noop-conv-x2", () -> new Column[] {NOOP.apply(q), NOOP.apply(q).alias("b")});
    v.put("noop-key-x2", () -> new Column[] {KEY.apply(q), KEY.apply(q).alias("b")});
    v.put("code-only", () -> new Column[] {k.compute(C, q)});
    v.put("both", () -> new Column[] {k.compute(C, q), k.compute(V, q)});
    v.put("both-narrow", () -> new Column[] {k.compute(C, narrow), k.compute(V, narrow)});
    final String only = System.getProperty("only");
    for (final Map.Entry<String, Supplier<Column[]>> e : v.entrySet()) {
      if (only != null && !only.equals(e.getKey())) continue;
      final Dataset<Row> query = obs.select(e.getValue().get());
      query.write().format("noop").mode("overwrite").save();
      final long[] t = new long[runs];
      for (int i = 0; i < runs; i++) {
        final long s = System.nanoTime();
        query.write().format("noop").mode("overwrite").save();
        t[i] = (System.nanoTime() - s) / 1_000_000;
      }
      Arrays.sort(t);
      System.out.printf("RESULT %s median_ms=%d runs=%s%n", e.getKey(), t[runs / 2], Arrays.toString(t));
    }
  }
}
