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
package au.csiro.pathling.legacy;

import au.csiro.pathling.encoders.ExtensionSupport;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.encoders.ResourceTypes;
import jakarta.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.instance.model.api.IBaseResource;

/**
 * Chooses the legacy encoder that reads every column of a legacy-layout table.
 *
 * <p>The table does not record how the encoder that wrote it was configured, and the decoder reads
 * its columns by name, ignoring any it does not know. Decoding with a narrower configuration than
 * the one the table was written with would therefore drop data without a word, so the decoder is
 * configured to be at least as wide as the table:
 *
 * <ul>
 *   <li>Extensions are enabled where the table carries the extension column at its root.
 *   <li>Every open type is enabled. A value type the table does not carry is simply not read.
 *   <li>The nesting level is the lowest at which the decoder reads every column, found by trying
 *       each in turn up to {@value #MAX_NESTING_LEVEL}.
 * </ul>
 *
 * <p>A table that still holds a column the decoder would not read is refused, naming the columns.
 * This is what catches a table of another resource type, or one with columns added after it was
 * written.
 *
 * @author Piotr Szul
 */
final class LegacyDecoder {

  /** The deepest nesting level tried. It is well beyond any level the library defaulted to. */
  static final int MAX_NESTING_LEVEL = 10;

  /** The largest number of unread columns a refusal names before abbreviating the rest. */
  private static final int MAX_CITED_COLUMNS = 10;

  private LegacyDecoder() {}

  /**
   * Returns the legacy encoder that reads every column of a legacy-layout table.
   *
   * @param resourceType the resource type the table holds
   * @param schema the schema of the table
   * @return the encoder
   * @throws LegacyExportException if the resource type is not one the legacy encoder encodes, or
   *     the table holds a column the encoder would not read
   */
  @Nonnull
  static ExpressionEncoder<IBaseResource> encoderFor(
      @Nonnull final String resourceType, @Nonnull final StructType schema) {
    if (!ResourceTypes.isSupported(resourceType)) {
      throw new LegacyExportException(
          resourceType + " is not a resource type the legacy encoder encodes.");
    }
    final boolean extensions =
        Arrays.asList(schema.fieldNames()).contains(ExtensionSupport.EXTENSIONS_FIELD_NAME());
    List<String> unread = List.of();
    for (int level = 0; level <= MAX_NESTING_LEVEL; level++) {
      final ExpressionEncoder<IBaseResource> encoder =
          FhirEncoders.forR4()
              .withMaxNestingLevel(level)
              .withExtensionsEnabled(extensions)
              .withAllOpenTypes()
              .getOrCreate()
              .of(resourceType);
      final List<String> remaining = SchemaCoverage.unread(schema, encoder.schema());
      if (remaining.isEmpty()) {
        return encoder;
      }
      // A deeper level reads only deeper columns, so where it has read none of those the previous
      // level left, no level will, and the table is refused without trying the rest.
      if (remaining.equals(unread)) {
        break;
      }
      unread = remaining;
    }
    throw new LegacyExportException(
        "The "
            + resourceType
            + " table holds columns that the legacy encoder for "
            + resourceType
            + " does not read, so exporting it would drop them: "
            + cite(unread)
            + ". Check that the table holds "
            + resourceType
            + " resources, was written by the Pathling FHIR encoders with no more than "
            + MAX_NESTING_LEVEL
            + " levels of nesting, and has had no columns added since.");
  }

  @Nonnull
  private static String cite(@Nonnull final List<String> columns) {
    final String cited = String.join(", ", columns.stream().limit(MAX_CITED_COLUMNS).toList());
    return columns.size() > MAX_CITED_COLUMNS
        ? cited + " and " + (columns.size() - MAX_CITED_COLUMNS) + " more"
        : cited;
  }
}
