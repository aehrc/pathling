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
import au.csiro.pathling.schema.SchemaConfiguration;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import org.apache.spark.sql.SparkSession;

/**
 * Fixtures shared by the transform tests: the R4 definitions, the bounds that apply to the dense
 * mode, a Spark session, and the writing of a corpus of FHIR JSON documents to a directory.
 *
 * <p>It is public because the round trip harness in the parent package drives the same transform.
 *
 * <p>The corpus is written as newline-delimited JSON files, because that is the ingest path on
 * which Spark preserves the lexical form of a number. Reading a dataset of strings routes the value
 * through a double and is documented as lossy (FR-020), so no test here uses it.
 */
public final class TransformFixtures {

  /** The FHIR R4 definitions, built once because the HAPI context is expensive to create. */
  @Nonnull
  public static final DefinitionContext DEFINITIONS = FhirDefinitionContext.of(FhirContext.forR4());

  /**
   * The open types Pathling enables by default. They are repeated here rather than taken from the
   * encoders module, which this module must not depend upon.
   */
  @Nonnull
  public static final Set<String> STANDARD_OPEN_TYPES =
      Set.of(
          "boolean",
          "code",
          "date",
          "dateTime",
          "decimal",
          "integer",
          "string",
          "Coding",
          "CodeableConcept",
          "Address",
          "Identifier",
          "Reference");

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

  /**
   * Returns a transformer over the R4 definitions, bounded as the defaults bound the dense mode.
   */
  @Nonnull
  public static ResourceTransformer transformer(@Nonnull final SchemaConfiguration configuration) {
    return ResourceTransformer.of(DEFINITIONS, configuration, 0, false, STANDARD_OPEN_TYPES);
  }

  /** Returns a transformer in the default configuration, which ignores non-conformant content. */
  @Nonnull
  public static ResourceTransformer transformer() {
    return transformer(SchemaConfiguration.builder().build());
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
