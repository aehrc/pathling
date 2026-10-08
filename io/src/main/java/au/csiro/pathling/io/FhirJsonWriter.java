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

package au.csiro.pathling.io;

import jakarta.annotation.Nonnull;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.functions;

/**
 * Writes resources stored in this layout as FHIR JSON text, to a dataset of documents or to files
 * of newline-delimited JSON (decisions 70 and 84). It is obtained from {@link FhirWriter#json()}.
 *
 * <p>A field which is null is left out rather than written as null, which is what FR-019 now asks
 * for. The rest of FR-019 is deferred (decision 71): a structure whose every field is null is
 * written as an empty object and an array holding nulls as it stands, neither of which is
 * conformant FHIR. For conformant input that happens only where a primitive's id and extensions
 * were not stored, and is kept on purpose until M5 (decision 71's addendum); the layout is assumed
 * to carry neither for any other reason.
 */
public final class FhirJsonWriter implements FhirFormatWriter {

  /** Asks the JSON writer to leave out a field that is null. */
  @Nonnull
  private static final Map<String, String> WRITE_OPTIONS = Map.of("ignoreNullFields", "true");

  @Nonnull private static final String DOCUMENT = "document";

  @Nonnull private final ResourceTransformer transformer;

  FhirJsonWriter(@Nonnull final ResourceTransformer transformer) {
    this.transformer = transformer;
  }

  @Override
  @Nonnull
  public Dataset<String> write(
      @Nonnull final String resourceType, @Nonnull final Dataset<Row> stored) {
    final Dataset<Row> shaped = transformer.toJsonShape(resourceType, stored);
    final Column document =
        functions.to_json(
            functions.struct(Stream.of(shaped.columns()).map(shaped::col).toArray(Column[]::new)),
            WRITE_OPTIONS);
    return shaped.select(document.alias(DOCUMENT)).as(Encoders.STRING());
  }

  /**
   * Writes one FHIR JSON document per stored resource to files of newline-delimited JSON. The
   * documents are written by the JSON writer directly, with no dataset of text between.
   *
   * @param resourceType the type of the resources the dataset carries
   * @param stored the resources, in this layout
   * @param path the directory to write to
   * @param mode what to do where the directory already exists
   */
  public void write(
      @Nonnull final String resourceType,
      @Nonnull final Dataset<Row> stored,
      @Nonnull final String path,
      @Nonnull final SaveMode mode) {
    transformer
        .toJsonShape(resourceType, stored)
        .write()
        .options(WRITE_OPTIONS)
        .mode(mode)
        .json(path);
  }
}
