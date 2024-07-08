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
import java.util.Objects;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Coding;
import scala.collection.JavaConverters;
import scala.collection.mutable.WrappedArray;

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

  @SuppressWarnings("unchecked")
  @Nullable
  public static Stream<Coding> decodeOneOrMany(final @Nullable Object codingRowOrArray,
      final int argumentIndex) {
    if (codingRowOrArray instanceof WrappedArray<?>) {
      return decodeMany((WrappedArray<Row>) codingRowOrArray);
    } else if (codingRowOrArray instanceof Row || codingRowOrArray == null) {
      return decodeOne((Row) codingRowOrArray);
    } else {
      throw new IllegalArgumentException(
          String.format("Row or WrappedArray<Row> column expected in argument %s, but given: %s,",
              argumentIndex, codingRowOrArray.getClass()));
    }
  }

  @Nullable
  public static Stream<Coding> decodeOneOrMany(final @Nullable Object codingRowOrArray) {
    return decodeOneOrMany(codingRowOrArray, 0);
  }

  @Nullable
  public static Stream<Coding> decodeOne(final @Nullable Row codingRow) {
    return codingRow != null
           ? Stream.of(CodingEncoding.decode(codingRow))
           : null;
  }

  @Nullable
  public static Stream<Coding> decodeMany(final @Nullable WrappedArray<Row> codingsRow) {
    return codingsRow != null
           ? JavaConverters.asJavaCollection(codingsRow).stream().filter(Objects::nonNull)
               .map(CodingEncoding::decode)
           : null;
  }

  public static boolean isValidCoding(@Nullable final Coding coding) {
    return nonNull(coding) && nonNull(coding.getSystem()) && nonNull(coding.getCode());
  }

  @Nonnull
  public static Stream<Coding> validCodings(@Nonnull final Stream<Coding> codings) {
    return codings.filter(TerminologyUdfHelpers::isValidCoding);
  }
}
