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

package au.csiro.pathling.util;

import au.csiro.pathling.config.EncodingConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import jakarta.annotation.Nonnull;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseResource;

/**
 * Test fixtures that produce a pair of FHIR encoders differing only in their configured open types,
 * so that a Delta table can be seeded at one encoder's schema and then written to or read by the
 * other.
 *
 * <p>The pair is derived from the encoders under test rather than from a fixed configuration. The
 * wide encoder is the one supplied; the narrow encoder is the same configuration with {@code
 * Period} and {@code Quantity} removed from its open types, which drops the corresponding {@code
 * valuePeriod} and {@code valueQuantity} fields from the extension element struct. Deriving the
 * pair this way matters because the encoding configuration differs between the server's shipped
 * defaults and the test profile, so a hardcoded pair would describe neither reliably.
 *
 * <p>A table seeded with the wide encoder is therefore wider than the narrow encoder's schema in
 * exactly the way a warehouse is when {@code pathling.encoding.openTypes} has been narrowed against
 * populated data, which is the state reported in issue #2697. The difference is purely narrowing:
 * the narrow schema introduces nothing the wide one lacks.
 *
 * <p>This is the counterpart of {@link DeltaSchemaFixtures}, which produces the opposite direction
 * by rewriting a committed schema on disk to remove fields.
 *
 * @author John Grimes
 */
public final class FhirEncoderFixtures {

  /** The open types the narrow encoder drops relative to the encoders it is derived from. */
  public static final Set<String> NARROWED_OPEN_TYPES = Set.of("Period", "Quantity");

  /**
   * The roots of the field subtrees that exist only in the wide encoder's schema. A narrow server
   * sees these, and every path beneath them, as excess when it reads a table written by a wide one,
   * so these are the paths its messages are expected to name.
   */
  public static final Set<String> WIDE_ONLY_FIELD_PATH_ROOTS =
      Set.of("_extension.valuePeriod", "_extension.valueQuantity");

  private FhirEncoderFixtures() {}

  /**
   * Returns encoders matching the given ones except that {@code Period} and {@code Quantity} are
   * removed from the open types. Every other setting is carried across unchanged, so the resulting
   * schema differs only in the extension value fields.
   *
   * @param encoders the encoders to narrow, typically the ones the application context provides
   * @return the narrowed encoders
   * @throws IllegalArgumentException if the given encoders carry neither of the narrowed open
   *     types, in which case narrowing would produce no difference and the fixture would prove
   *     nothing
   */
  @Nonnull
  public static FhirEncoders narrow(@Nonnull final FhirEncoders encoders) {
    final EncodingConfiguration configuration = encoders.getConfiguration();
    final Set<String> openTypes = new HashSet<>(configuration.getOpenTypes());
    if (!openTypes.removeAll(NARROWED_OPEN_TYPES)) {
      throw new IllegalArgumentException(
          "The encoders to narrow must carry at least one of " + NARROWED_OPEN_TYPES);
    }
    return FhirEncoders.forR4()
        .withOpenTypes(openTypes)
        .withMaxNestingLevel(configuration.getMaxNestingLevel())
        .withExtensionsEnabled(configuration.isEnableExtensions())
        .getOrCreate();
  }

  /**
   * Seeds a Delta table at the schema of the nominated encoder, failing if a table already exists
   * at the path.
   *
   * @param spark the Spark session
   * @param encoders the encoders whose schema the table is written at
   * @param resourceCode the resource type code being written
   * @param resources the resources to write, which may be empty to seed an empty table
   * @param tablePath the path to write the Delta table to
   * @param <T> the resource type being written
   */
  public static <T extends IBaseResource> void seedTable(
      @Nonnull final SparkSession spark,
      @Nonnull final FhirEncoders encoders,
      @Nonnull final String resourceCode,
      @Nonnull final List<T> resources,
      @Nonnull final String tablePath) {
    encodeResources(spark, encoders, resourceCode, resources)
        .write()
        .format("delta")
        .mode(SaveMode.ErrorIfExists)
        .save(tablePath);
  }

  /**
   * Encodes resources using the nominated encoder, without writing them anywhere. Useful for
   * obtaining an encoder's schema, or for merging into an existing table.
   *
   * @param spark the Spark session
   * @param encoders the encoders to use
   * @param resourceCode the resource type code being encoded
   * @param resources the resources to encode, which may be empty
   * @param <T> the resource type being encoded
   * @return the encoded dataset
   */
  @Nonnull
  public static <T extends IBaseResource> Dataset<Row> encodeResources(
      @Nonnull final SparkSession spark,
      @Nonnull final FhirEncoders encoders,
      @Nonnull final String resourceCode,
      @Nonnull final List<T> resources) {
    return spark.createDataset(resources, encoders.<T>of(resourceCode)).toDF();
  }
}
