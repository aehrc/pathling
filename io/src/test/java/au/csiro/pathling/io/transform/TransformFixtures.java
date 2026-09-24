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

package au.csiro.pathling.io.transform;

import au.csiro.pathling.definition.DefinitionContext;
import au.csiro.pathling.definition.fhir.FhirDefinitionContext;
import au.csiro.pathling.io.json.FhirJsonReader;
import au.csiro.pathling.io.json.FhirJsonWriter;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Fixtures shared by the transform tests: the R4 definitions, a Spark session, the transformer,
 * reader and writer over them, and the writing of a corpus of FHIR JSON documents to a directory.
 *
 * <p>It is public because the round trip harness in the parent package drives the same transform.
 */
public final class TransformFixtures {

  /** The FHIR R4 definitions, built once because the HAPI context is expensive to create. */
  @Nonnull
  public static final DefinitionContext DEFINITIONS = FhirDefinitionContext.of(FhirContext.forR4());

  @Nullable private static SparkSession session;

  private TransformFixtures() {}

  /**
   * Returns the Spark session shared by the transform tests, created on first use. It is not
   * stopped between classes, because a Surefire fork runs several of them and a session is
   * expensive to build.
   */
  @Nonnull
  public static synchronized SparkSession spark() {
    if (session == null) {
      session =
          SparkSession.builder()
              .master("local[2]")
              .appName("io-transform-testing")
              .config("spark.driver.bindAddress", "localhost")
              .config("spark.driver.host", "localhost")
              .config("spark.ui.enabled", "false")
              .config("spark.sql.shuffle.partitions", "1")
              .getOrCreate();
    }
    return session;
  }

  /** Returns a transformer over the R4 definitions. */
  @Nonnull
  public static ResourceTransformer transformer() {
    return ResourceTransformer.of(DEFINITIONS);
  }

  /** Returns a reader over the R4 definitions and the shared session. */
  @Nonnull
  public static FhirJsonReader reader() {
    return FhirJsonReader.of(spark(), transformer());
  }

  /** Returns a writer over the R4 definitions. */
  @Nonnull
  public static FhirJsonWriter writer() {
    return FhirJsonWriter.of(transformer());
  }

  /**
   * Reads newline-delimited JSON with an inferred schema and nothing imposed on it, failing on text
   * that is not JSON as the reader does. It is what the transform's findings are asked about.
   */
  @Nonnull
  public static Dataset<Row> inferred(@Nonnull final String path) {
    return spark().read().option("mode", "FAILFAST").json(path);
  }

  /**
   * Writes a corpus of FHIR JSON documents to a directory as newline-delimited JSON, returning the
   * path the reader is given.
   */
  @Nonnull
  public static String corpus(@Nonnull final Path directory, @Nonnull final String... documents) {
    final Path file = directory.resolve("resources.ndjson");
    try {
      Files.write(file, List.of(documents));
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
    return directory.toString();
  }
}
