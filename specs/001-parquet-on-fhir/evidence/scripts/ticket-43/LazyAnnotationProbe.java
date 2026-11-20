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

import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.schema.SchemaConfiguration;
import au.csiro.pathling.search.SearchColumnBuilder;
import ca.uhn.fhir.context.FhirContext;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Times the engine's date paths through the public API over unannotated and annotated Patients, so
 * that the same probe runs against a build before ticket 43 (a date's range resolved as it is
 * traversed) and after it (a bound read through an accessor only where an operation reads it). It
 * extends ticket 42's probe with the annotated data, a union of dates and a deduplication. Run by
 * run.sh, once per jar.
 *
 * <p>Usage: LazyAnnotationProbe <work dir> <rows> <runs>.
 */
public final class LazyAnnotationProbe {

  private LazyAnnotationProbe() {}

  public static void main(final String[] args) throws Exception {
    final Path work = Path.of(args[0]);
    final int rows = Integer.parseInt(args[1]);
    final int runs = Integer.parseInt(args[2]);
    final SparkSession spark =
        SparkSession.builder()
            .master(System.getProperty("master", "local[4]"))
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.adaptive.enabled", "false")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.ui.enabled", "false")
            // Annotated timestamps need statistics for a pushed filter to skip row groups (ticket
            // 44).
            .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
            .getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");
    final SchemaConfiguration none =
        SchemaConfiguration.builder()
            .enableNumericAnnotation(false)
            .enableRangeAnnotation(false)
            .enableCanonicalAnnotation(false)
            .build();
    final PathlingContext unannotating = PathlingContext.builder(spark).schemaConfiguration(none).build();
    final PathlingContext annotating = PathlingContext.create(spark);

    // The same data for every jar: unannotated Patients written once to Parquet.
    final Path data = work.resolve("patients-" + rows);
    if (!Files.exists(data)) {
      final Dataset<String> json = spark.createDataset(patients(rows), Encoders.STRING());
      unannotating.encode(json.toDF(), "Patient").write().parquet(data.toString());
    }
    final Dataset<Row> unannotatedPatients = spark.read().parquet(data.toString());
    // The same Patients annotated, written once.
    final Path annotatedData = work.resolve("patients-annotated-" + rows);
    if (!Files.exists(annotatedData)) {
      annotating
          .annotate(unannotatedPatients, "Patient")
          .write()
          .parquet(annotatedData.toString());
    }
    final Dataset<Row> annotatedPatients = spark.read().parquet(annotatedData.toString());
    final SearchColumnBuilder builder =
        SearchColumnBuilder.withDefaultRegistry(FhirContext.forR4());

    final List<String[]> contexts =
        List.of(
            new String[] {"compare-singular", "birthDate > @1980"},
            new String[] {"compare-in-lambda", "name.where(period.start > @2001-06).count()"},
            new String[] {"output-in-lambda", "name.period.start"},
            new String[] {"union", "(name.period.start | birthDate).count()"},
            new String[] {"distinct", "name.period.start.distinct().count()"},
            new String[] {"string-union", "(name.family | name.family).count()"},
            new String[] {"complex-union", "(name | name).count()"},
            new String[] {"string-search", "family=F1"},
            new String[] {"search", "birthdate=ge1980"},
            new String[] {"filter", "birthDate >= @1980"});
    for (final String layout : List.of("unannotated", "annotated")) {
      final Dataset<Row> patients =
          layout.equals("annotated") ? annotatedPatients : unannotatedPatients;
      for (final String[] context : contexts) {
        final Dataset<Row> query =
            switch (context[0]) {
              case "search", "string-search" ->
                  patients.filter(builder.fromQueryString(ResourceType.PATIENT, context[1]));
              case "filter" ->
                  patients.filter(builder.fromExpression(ResourceType.PATIENT, context[1]));
              default ->
                  patients.select(builder.fromExpression(ResourceType.PATIENT, context[1]));
            };
        final long[] times = new long[runs];
        run(query);
        for (int i = 0; i < runs; i++) {
          final long start = System.nanoTime();
          run(query);
          times[i] = (System.nanoTime() - start) / 1_000_000;
        }
        Arrays.sort(times);
        System.out.printf(
            "RESULT %s/%s median_ms=%d min_ms=%d max_ms=%d runs=%s%n",
            layout, context[0], times[runs / 2], times[0], times[runs - 1], Arrays.toString(times));
      }
    }
    spark.stop();
  }

  private static void run(final Dataset<Row> query) {
    query.write().format("noop").mode("overwrite").save();
  }

  /**
   * Patients whose dates are drawn from a pool of 5,000, of a mix of precisions, as the design's
   * memoisation probe draws them: a birth date and three names, each with a period start.
   */
  private static List<String> patients(final int rows) {
    final Random random = new Random(42);
    final List<String> pool = new ArrayList<>();
    for (int i = 0; i < 5000; i++) {
      final int year = 1900 + random.nextInt(120);
      final int month = 1 + random.nextInt(12);
      final int day = 1 + random.nextInt(28);
      final double kind = random.nextDouble();
      pool.add(
          kind < 0.2
              ? String.valueOf(year)
              : kind < 0.4
                  ? String.format("%d-%02d", year, month)
                  : kind < 0.7
                      ? String.format("%d-%02d-%02d", year, month, day)
                      : String.format(
                          "%d-%02d-%02dT%02d:%02d:%02d%s",
                          year,
                          month,
                          day,
                          random.nextInt(24),
                          random.nextInt(60),
                          random.nextInt(60),
                          List.of("Z", "+10:00", "-05:00").get(random.nextInt(3))));
    }
    final List<String> result = new ArrayList<>(rows);
    for (int i = 0; i < rows; i++) {
      final String birth = pool.get(random.nextInt(pool.size())).substring(0, 4);
      final StringBuilder names = new StringBuilder();
      for (int n = 0; n < 3; n++) {
        if (n > 0) {
          names.append(',');
        }
        names
            .append("{\"family\":\"F")
            .append(n)
            .append("\",\"period\":{\"start\":\"")
            .append(pool.get(random.nextInt(pool.size())))
            .append("\"}}");
      }
      result.add(
          "{\"resourceType\":\"Patient\",\"id\":\"p"
              + i
              + "\",\"birthDate\":\""
              + birth
              + "-"
              + String.format("%02d-%02d", 1 + random.nextInt(12), 1 + random.nextInt(28))
              + "\",\"name\":["
              + names
              + "]}");
    }
    return result;
  }
}
