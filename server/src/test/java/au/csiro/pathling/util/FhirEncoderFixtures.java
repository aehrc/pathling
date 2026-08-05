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
 * <p>The wide encoder adds {@code Period} and {@code Quantity} to the standard open types, which
 * gives the extension element struct the additional {@code valuePeriod} and {@code valueQuantity}
 * fields. A table seeded with the wide encoder is therefore wider than the narrow encoder's schema
 * in exactly the way a warehouse is when {@code pathling.encoding.openTypes} has been narrowed
 * against populated data, which is the state reported in issue #2697.
 *
 * <p>This is the counterpart of {@link DeltaSchemaFixtures}, which produces the opposite direction
 * by rewriting a committed schema on disk to remove fields.
 *
 * @author John Grimes
 */
public final class FhirEncoderFixtures {

  /** The open types added by the wide encoder, on top of the standard set. */
  public static final Set<String> ADDITIONAL_OPEN_TYPES = Set.of("Period", "Quantity");

  /**
   * The nesting level both encoders are built with, matching the default of {@code
   * pathling.encoding.maxNestingLevel}. The encoder builder's own default is 0, so this must be set
   * explicitly for the fixture to describe the same schema the server does.
   */
  private static final int MAX_NESTING_LEVEL = 3;

  /**
   * Both encoders are built with extensions enabled, matching the default of {@code
   * pathling.encoding.enableExtensions}. This is what makes the open types observable at all: the
   * open-type value fields live inside the {@code _extension} element, so with extensions disabled
   * the two encoders would produce identical schemas.
   */
  private static final boolean ENABLE_EXTENSIONS = true;

  /**
   * The roots of the field subtrees that exist only in the wide encoder's schema. A narrow server
   * sees these, and every path beneath them, as excess when it reads a table written by a wide one,
   * so these are the paths its messages are expected to name.
   */
  public static final Set<String> WIDE_ONLY_FIELD_PATH_ROOTS =
      Set.of("_extension.valuePeriod", "_extension.valueQuantity");

  private FhirEncoderFixtures() {}

  /**
   * Returns encoders configured with the standard open types plus {@code Period} and {@code
   * Quantity}. Every other setting matches the server's defaults, so the schema differs from {@link
   * #narrowEncoders()} only in the extension value fields.
   *
   * @return the wide encoders
   */
  @Nonnull
  public static FhirEncoders wideEncoders() {
    final Set<String> openTypes = new HashSet<>(FhirEncoders.STANDARD_OPEN_TYPES);
    openTypes.addAll(ADDITIONAL_OPEN_TYPES);
    return encoders(openTypes);
  }

  /**
   * Returns encoders configured with the standard open types, which is the server's shipped
   * default.
   *
   * @return the narrow encoders
   */
  @Nonnull
  public static FhirEncoders narrowEncoders() {
    return encoders(FhirEncoders.STANDARD_OPEN_TYPES);
  }

  /** Builds encoders for the given open types, with every other setting at the server's default. */
  @Nonnull
  private static FhirEncoders encoders(@Nonnull final Set<String> openTypes) {
    return FhirEncoders.forR4()
        .withOpenTypes(openTypes)
        .withMaxNestingLevel(MAX_NESTING_LEVEL)
        .withExtensionsEnabled(ENABLE_EXTENSIONS)
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
