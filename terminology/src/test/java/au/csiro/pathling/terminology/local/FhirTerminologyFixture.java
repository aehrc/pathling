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

package au.csiro.pathling.terminology.local;

import au.csiro.pathling.config.LocalTerminologyConfiguration;
import au.csiro.pathling.config.TerminologyConfiguration;
import au.csiro.pathling.config.TerminologyMode;
import au.csiro.pathling.terminology.store.FhirTerminologyImporter;
import au.csiro.pathling.test.FhirFixtures;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.Map;
import org.apache.spark.sql.SparkSession;

/**
 * Imports the FHIR animal-species fixtures into a temporary store exactly once for the whole test
 * JVM, so the several FHIR-content test classes share a single import. The store is read through
 * Delta Kernel, which has no Spark dependency, so the reader remains usable after the import.
 *
 * @author John Grimes
 */
final class FhirTerminologyFixture {

  private static String storagePath;

  private FhirTerminologyFixture() {
    // Static holder.
  }

  static synchronized void ensure() {
    if (storagePath != null) {
      return;
    }
    final SparkSession spark =
        SparkSession.builder()
            .appName("FhirTerminologyFixture")
            .master("local[2]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.driver.bindAddress", "localhost")
            .config("spark.driver.host", "localhost")
            .config("spark.ui.enabled", "false")
            .getOrCreate();
    final String path;
    try {
      path = Files.createTempDirectory("fhir-fixture-store").resolve("store").toString();
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
    new FhirTerminologyImporter(spark, path).importFrom(FhirFixtures.jsonDirectory().toString());
    storagePath = path;
  }

  @Nonnull
  static String storagePath() {
    ensure();
    return storagePath;
  }

  @Nonnull
  static LocalTerminologyService service() {
    ensure();
    final TerminologyConfiguration configuration =
        TerminologyConfiguration.builder()
            .mode(TerminologyMode.LOCAL)
            .local(LocalTerminologyConfiguration.builder().storagePath(storagePath).build())
            .build();
    return new LocalTerminologyService(configuration, Map.of());
  }
}
