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

package au.csiro.pathling.library.io;

import au.csiro.pathling.config.EncodingConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.encoders.utils.SchemaMisalignment;
import au.csiro.pathling.library.PathlingContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Set;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Patient;

/**
 * Test support for the library API tests that need two encoder configurations differing only in
 * their open types, and a way to move resources in and out of them.
 *
 * <p>The two configurations are the ones {@link SchemaMisalignment} defines for the {@code
 * encoders} module, so the projection-based tests there and the Delta-level tests here are
 * describing the same pair of encoders.
 *
 * @author John Grimes
 */
final class ExtensionContexts {

  /** The MIME type used when moving decoded resources back out as strings. */
  private static final String FHIR_JSON = "application/fhir+json";

  private ExtensionContexts() {}

  /**
   * Returns a context configured with the narrow set of open types.
   *
   * @param spark the Spark session to use
   * @return the narrow context
   */
  @Nonnull
  static PathlingContext narrow(@Nonnull final SparkSession spark) {
    return contextFor(spark, SchemaMisalignment.NARROW_OPEN_TYPES);
  }

  /**
   * Returns a context configured with the wide set of open types.
   *
   * @param spark the Spark session to use
   * @return the wide context
   */
  @Nonnull
  static PathlingContext wide(@Nonnull final SparkSession spark) {
    return contextFor(spark, SchemaMisalignment.WIDE_OPEN_TYPES);
  }

  /**
   * Encodes the given resources with the narrow encoder.
   *
   * @param spark the Spark session to use
   * @param patients the resources to encode
   * @return the encoded dataset
   */
  @Nonnull
  static Dataset<Row> encodeNarrow(
      @Nonnull final SparkSession spark, @Nonnull final List<Patient> patients) {
    return spark
        .createDataset(patients, SchemaMisalignment.narrowEncoders().of(Patient.class))
        .toDF();
  }

  /**
   * Encodes the given resources with the wide encoder.
   *
   * @param spark the Spark session to use
   * @param patients the resources to encode
   * @return the encoded dataset
   */
  @Nonnull
  static Dataset<Row> encodeWide(
      @Nonnull final SparkSession spark, @Nonnull final List<Patient> patients) {
    return spark
        .createDataset(patients, SchemaMisalignment.wideEncoders().of(Patient.class))
        .toDF();
  }

  /**
   * Decodes the first row of the given dataset through the library API, which is the path a caller
   * takes when reading resources back out of storage.
   *
   * @param context the context whose encoder configuration decodes the data
   * @param dataset the dataset to decode
   * @return the decoded resource
   */
  @Nonnull
  static Patient decodeOne(
      @Nonnull final PathlingContext context, @Nonnull final Dataset<Row> dataset) {
    final String json = context.decode(dataset, "Patient", FHIR_JSON).head();
    return (Patient)
        FhirEncoders.contextFor(FhirVersionEnum.R4).newJsonParser().parseResource(json);
  }

  @Nonnull
  private static PathlingContext contextFor(
      @Nonnull final SparkSession spark, @Nonnull final Set<String> openTypes) {
    return PathlingContext.createForEncoding(
        spark, EncodingConfiguration.builder().enableExtensions(true).openTypes(openTypes).build());
  }
}
