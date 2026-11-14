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

package au.csiro.pathling.fhirpath.column;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.filter;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.transform;
import static org.assertj.core.api.Assertions.assertThat;

import au.csiro.pathling.fhirpath.comparison.TemporalComparator;
import au.csiro.pathling.fhirpath.encoding.QuantityEncoding;
import au.csiro.pathling.schema.annotation.kinds.DateRangeKind;
import au.csiro.pathling.schema.annotation.kinds.QuantityCanonicalKind;
import au.csiro.pathling.search.filter.DateMatcher;
import au.csiro.pathling.test.SpringBootUnitTest;
import jakarta.annotation.Nonnull;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.List;
import java.util.function.LongSupplier;
import java.util.stream.IntStream;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Tests that the annotations computed from one value share one memoised computation of it (decision
 * 145, design §4.6): the two bounds of a date one parse, and the two canonical annotations of a
 * quantity one canonicalisation, wherever the plan computes them.
 *
 * <p>Every value is distinct, and distinct from those of every other test, so that no computation
 * is spared by a value cached before. The count is then the number of computations per value, the
 * measurement of {@code evidence/ticket-42-memoisation.md}.
 */
@SpringBootUnitTest
class AnnotationMemoisationTest {

  /** The number of rows, each with a date and an array of {@link #ELEMENTS} more. */
  private static final int ROWS = 500;

  private static final int ELEMENTS = 3;

  @Autowired SparkSession spark;

  @TempDir Path tempDir;

  private static final DateRangeKind KIND = DateRangeKind.INSTANCE;

  /**
   * Builds rows of a date, {@code t}, and an array of dates, {@code a}, all distinct, from a year
   * of their own, so that each test parses dates no other has.
   */
  @Nonnull
  private Dataset<Row> dates(final int year) {
    final StructType schema =
        new StructType()
            .add("t", DataTypes.StringType)
            .add("a", new ArrayType(DataTypes.StringType, true));
    final LocalDate first = LocalDate.of(year, 1, 1);
    final Dataset<Row> rows =
        spark.createDataFrame(
            IntStream.range(0, ROWS)
                .mapToObj(
                    i ->
                        RowFactory.create(
                            first.plusDays(i * (ELEMENTS + 1L)).toString(),
                            IntStream.rangeClosed(1, ELEMENTS)
                                .mapToObj(j -> first.plusDays(i * (ELEMENTS + 1L) + j).toString())
                                .toArray(String[]::new)))
                .toList(),
            schema);
    // The rows are written to Parquet and read back, so that the query runs in Spark's tasks, as it
    // does over stored data, rather than being folded over the local rows on the driver.
    final String path = tempDir.resolve("dates-" + year).toString();
    rows.repartition(4).write().parquet(path);
    return spark.read().parquet(path);
  }

  /** Runs a query and returns how many computations it made, as the counter counts them. */
  private static long computations(
      @Nonnull final Dataset<Row> query, @Nonnull final LongSupplier counter) {
    final long before = counter.getAsLong();
    query.write().format("noop").mode("overwrite").save();
    return counter.getAsLong() - before;
  }

  private static long parses(@Nonnull final Dataset<Row> query) {
    return computations(query, DateRangeKind::getParseCount);
  }

  @Test
  void bothBoundsAsTwoColumnsParseEachDateOnce() {
    final Dataset<Row> query = dates(1101).select(KIND.start(col("t")), KIND.end(col("t")));
    assertThat(parses(query)).isEqualTo(ROWS);
  }

  @Test
  void aComparisonOfASingularDateParsesItOnce() {
    // The comparison reads the lower bound three times and the upper bound twice.
    final Dataset<Row> query =
        dates(1111)
            .select(
                TemporalComparator.forDateTime()
                    .lessThan(
                        SystemDateTime.fromText(col("t")), SystemDateTime.fromLiteral("1500")));
    assertThat(parses(query)).isEqualTo(ROWS);
  }

  @Test
  void aDateSearchParsesEachDateOnce() {
    final Dataset<Row> query =
        dates(1121)
            .select(
                DateMatcher.forScalarDates().match(SystemDateTime.fromText(col("t")), "eq1121-02"));
    assertThat(parses(query)).isEqualTo(ROWS);
  }

  @Test
  void bothBoundsInsideALambdaParseEachDateOnce() {
    final Dataset<Row> query =
        dates(1131).select(transform(col("a"), x -> struct(KIND.start(x), KIND.end(x))));
    assertThat(parses(query)).isEqualTo((long) ROWS * ELEMENTS);
  }

  @Test
  void aComparisonInsideALambdaParsesEachDateOnce() {
    final Dataset<Row> query =
        dates(1141)
            .select(
                filter(
                    col("a"),
                    x ->
                        TemporalComparator.forDateTime()
                            .greaterThan(
                                SystemDateTime.fromText(x),
                                SystemDateTime.fromLiteral("1141-06"))));
    assertThat(parses(query)).isEqualTo((long) ROWS * ELEMENTS);
  }

  @Test
  void theBoundsReadInSeparatePassesParseEachDateOnce() {
    final Dataset<Row> query =
        dates(1151).select(transform(col("a"), KIND::start), transform(col("a"), KIND::end));
    assertThat(parses(query)).isEqualTo((long) ROWS * ELEMENTS);
  }

  @Test
  void aRepeatingDateReadInTwoPassesParsesEachDateOnce() {
    // A repeating date without stored annotations computes each bound where it is read, here in a
    // pass of its own. Changed by ticket 43: traversal computes nothing, so the bounds are read
    // through the accessors rather than by converting the dates.
    final Column traversed = SystemDateTime.paired(col("a"), lit(null), List.of(), true);
    final Dataset<Row> query =
        dates(1161)
            .select(
                transform(traversed, SystemDateTime::lowerBound),
                transform(traversed, SystemDateTime::upperBound));
    assertThat(parses(query)).isEqualTo((long) ROWS * ELEMENTS);
  }

  @Test
  void bothCanonicalAnnotationsCanonicaliseEachQuantityOnce() {
    final Dataset<Row> quantities =
        spark
            .range(0, ROWS, 1, 4)
            .select(
                struct(
                        col("id").plus(lit(1_000_000_000L)).cast("string").alias("value"),
                        lit("http://unitsofmeasure.org").alias("system"),
                        lit("mg").alias("code"))
                    .alias("q"));
    final QuantityCanonicalKind kind = QuantityCanonicalKind.INSTANCE;
    final Dataset<Row> query =
        quantities.select(
            kind.compute(QuantityEncoding.CANONICAL_CODE_COLUMN, col("q")),
            kind.compute(QuantityEncoding.CANONICAL_VALUE_EXACT_COLUMN, col("q")));
    assertThat(computations(query, QuantityCanonicalKind::getCanonicalisationCount))
        .isEqualTo(ROWS);
  }

  @Test
  void aStoredQuantityTakenAsASystemQuantityIsCanonicalisedOnce() {
    // The engine's conversion of a stored quantity without annotations computes both canonical
    // annotations, each from the three fields read through the tolerant traversal.
    final String path = tempDir.resolve("quantities").toString();
    spark
        .range(0, ROWS, 1, 4)
        .select(
            struct(
                    col("id").plus(lit(2_000_000_000L)).cast("string").alias("value"),
                    lit("mg").alias("unit"),
                    lit("http://unitsofmeasure.org").alias("system"),
                    lit("mg").alias("code"))
                .alias("q"))
        .write()
        .parquet(path);
    final Column system = QuantityEncoding.fromStored(col("q"));
    final Dataset<Row> query =
        spark
            .read()
            .parquet(path)
            .select(
                QuantityEncoding.canonicalCode(system),
                QuantityEncoding.canonicalValueExact(system));
    assertThat(computations(query, QuantityCanonicalKind::getCanonicalisationCount))
        .isEqualTo(ROWS);
  }
}
