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
 * Times the engine's date and quantity annotation computations through the public API, so that the same probe runs against
 * a build before ticket 42 (one struct-returning computation per kind, bound once with bindOnce) and after
 * it (one scalar computation per annotation over a memoised parse). Run by run.sh, once per jar.
 *
 * <p>Usage: EngineAnnotationProbe <work dir> <rows> <runs>.
 */
public final class EngineAnnotationProbe {

  private EngineAnnotationProbe() {}

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
    final Dataset<Row> patients = spark.read().parquet(data.toString());
    final Path quantityData = work.resolve("observations-" + rows);
    if (!Files.exists(quantityData)) {
      final Dataset<String> json = spark.createDataset(observations(rows), Encoders.STRING());
      unannotating.encode(json.toDF(), "Observation").write().parquet(quantityData.toString());
    }
    final Dataset<Row> observations = spark.read().parquet(quantityData.toString());
    final SearchColumnBuilder builder =
        SearchColumnBuilder.withDefaultRegistry(FhirContext.forR4());

    final List<String[]> contexts =
        List.of(
            new String[] {"compare-singular", "birthDate > @1980"},
            new String[] {"compare-in-lambda", "name.where(period.start > @2001-06).count()"},
            new String[] {"output-in-lambda", "name.period.start"},
            new String[] {"search", "birthdate=ge1980"},
            new String[] {"annotate", ""},
            new String[] {"quantity-compare", "value.ofType(Quantity) > 50 'mg'"},
            new String[] {"quantity-annotate", ""});
    for (final String[] context : contexts) {
      final Dataset<Row> query =
          switch (context[0]) {
            case "search" ->
                patients.filter(builder.fromQueryString(ResourceType.PATIENT, context[1]));
            case "annotate" -> annotating.annotate(patients, "Patient");
            case "quantity-annotate" -> annotating.annotate(observations, "Observation");
            case "quantity-compare" ->
                observations.select(
                    builder.fromExpression(ResourceType.OBSERVATION, context[1]));
            default -> patients.select(builder.fromExpression(ResourceType.PATIENT, context[1]));
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
          "RESULT %s median_ms=%d min_ms=%d max_ms=%d runs=%s%n",
          context[0], times[runs / 2], times[0], times[runs - 1], Arrays.toString(times));
    }
    spark.stop();
  }

  private static void run(final Dataset<Row> query) {
    query.write().format("noop").mode("overwrite").save();
  }

  /** Observations with a UCUM quantity drawn from a pool of 5,000 values and three units. */
  private static List<String> observations(final int rows) {
    final Random random = new Random(43);
    final List<String> units = List.of("mg", "g", "kg");
    final List<String> result = new ArrayList<>(rows);
    for (int i = 0; i < rows; i++) {
      final String unit = units.get(random.nextInt(units.size()));
      result.add(
          "{\"resourceType\":\"Observation\",\"id\":\"o"
              + i
              + "\",\"status\":\"final\",\"code\":{\"text\":\"x\"},"
              + "\"valueQuantity\":{\"value\":"
              + (random.nextInt(5000) / 10.0)
              + ",\"unit\":\""
              + unit
              + "\",\"system\":\"http://unitsofmeasure.org\",\"code\":\""
              + unit
              + "\"}}");
    }
    return result;
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
