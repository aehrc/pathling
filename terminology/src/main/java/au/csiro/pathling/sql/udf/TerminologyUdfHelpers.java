/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.sql.udf;

import static java.util.Objects.nonNull;

import au.csiro.pathling.fhirpath.encoding.CodingEncoding;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Objects;
import java.util.stream.Stream;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Coding;
import scala.collection.Iterable;
import scala.collection.JavaConverters;

/**
 * Helper functions for terminology UDFs.
 */
public final class TerminologyUdfHelpers {

  private TerminologyUdfHelpers() {
    // Utility class
  }

  @Nullable
  static Row[] encodeMany(@Nullable final Stream<Coding> codings) {
    return codings != null
           ? codings.map(CodingEncoding::encode).toArray(Row[]::new)
           : null;
  }

  /**
   * Decodes one or many codings from a row or array object with argument index for error
   * reporting.
   *
   * @param codingRowOrArray the row or array object to decode
   * @param argumentIndex the argument index for error reporting
   * @return a stream of decoded codings, or null if input is null
   * @throws IllegalArgumentException if the input type is unexpected
   */
  @SuppressWarnings("unchecked")
  @Nullable
  public static Stream<Coding> decodeOneOrMany(final @Nullable Object codingRowOrArray,
      final int argumentIndex) {
    if (codingRowOrArray instanceof Iterable<?>) {
      return decodeMany((Iterable<Row>) codingRowOrArray);
    } else if (codingRowOrArray instanceof Row || codingRowOrArray == null) {
      return decodeOne((Row) codingRowOrArray);
    } else {
      throw new IllegalArgumentException(
          String.format("Row or WrappedArray<Row> column expected in argument %s, but given: %s,",
              argumentIndex, codingRowOrArray.getClass()));
    }
  }

  /**
   * Decodes one or many codings from a row or array object.
   *
   * @param codingRowOrArray the row or array object to decode
   * @return a stream of decoded codings, or null if input is null
   */
  @Nullable
  public static Stream<Coding> decodeOneOrMany(final @Nullable Object codingRowOrArray) {
    return decodeOneOrMany(codingRowOrArray, 0);
  }

  /**
   * Decodes a single coding from a row.
   *
   * @param codingRow the row containing the coding data
   * @return a stream containing the decoded coding, or null if input is null
   */
  @Nullable
  public static Stream<Coding> decodeOne(final @Nullable Row codingRow) {
    return codingRow != null
           ? Stream.of(CodingEncoding.decode(codingRow))
           : null;
  }

  /**
   * Decodes multiple codings from an iterable of rows.
   *
   * @param codingsRow the iterable containing the coding rows
   * @return a stream of decoded codings, or null if input is null
   */
  @Nullable
  public static Stream<Coding> decodeMany(final @Nullable Iterable<Row> codingsRow) {
    return codingsRow != null
           ? JavaConverters.asJavaCollection(codingsRow).stream().filter(Objects::nonNull)
               .map(CodingEncoding::decode)
           : null;
  }

  /**
   * Checks if a coding is valid (has non-null system and code).
   *
   * @param coding the coding to validate
   * @return true if the coding is valid, false otherwise
   */
  public static boolean isValidCoding(@Nullable final Coding coding) {
    return nonNull(coding) && nonNull(coding.getSystem()) && nonNull(coding.getCode());
  }

  /**
   * Filters a stream of codings to only include valid ones.
   *
   * @param codings the stream of codings to filter
   * @return a stream containing only valid codings
   */
  @Nonnull
  public static Stream<Coding> validCodings(@Nonnull final Stream<Coding> codings) {
    return codings.filter(TerminologyUdfHelpers::isValidCoding);
  }
}
