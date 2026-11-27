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
import java.util.function.UnaryOperator;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.types.DataType;

/**
 * How the values of one FHIR primitive type are stored, and how they are returned to JSON.
 *
 * <p>A converter states the type it stores its values as, and the two conversions. The input is
 * assumed to conform, so a value is converted from whatever type inference gave its column, and
 * what a non-conformant value becomes is undefined, as ingest in {@code trust} leaves it (decision
 * 133's addendum of 2026-10-22).
 */
final class PrimitiveConverter {

  @Nonnull private final DataType stored;

  @Nonnull private final UnaryOperator<Column> ingest;

  @Nonnull private final UnaryOperator<Column> egress;

  private PrimitiveConverter(
      @Nonnull final DataType stored,
      @Nonnull final UnaryOperator<Column> ingest,
      @Nonnull final UnaryOperator<Column> egress) {
    this.stored = stored;
    this.ingest = ingest;
    this.egress = egress;
  }

  /**
   * Returns a converter with conversions of its own.
   *
   * @param stored the type the values are stored as
   * @param ingest the conversion of a value to the stored type
   * @param egress the conversion of a stored value to what the JSON writer is given
   * @return the converter
   */
  @Nonnull
  public static PrimitiveConverter of(
      @Nonnull final DataType stored,
      @Nonnull final UnaryOperator<Column> ingest,
      @Nonnull final UnaryOperator<Column> egress) {
    return new PrimitiveConverter(stored, ingest, egress);
  }

  /**
   * Returns a converter that casts a value to the stored type, and gives the JSON writer the stored
   * value as it is.
   *
   * <p>The cast is a {@code try_cast}, so a value the stored type cannot represent becomes null
   * rather than raising or wrapping.
   *
   * @param stored the type the values are stored as
   * @return the converter
   */
  @Nonnull
  public static PrimitiveConverter casting(@Nonnull final DataType stored) {
    return new PrimitiveConverter(
        stored, value -> value.try_cast(stored), UnaryOperator.identity());
  }

  /**
   * Returns the type the values are stored as.
   *
   * @return the stored type
   */
  @Nonnull
  public DataType getStored() {
    return stored;
  }

  /**
   * Returns the stored value of a value.
   *
   * @param value the value as inferred
   * @return the stored value
   */
  @Nonnull
  public Column ingest(@Nonnull final Column value) {
    return ingest.apply(value);
  }

  /**
   * Returns what the JSON writer is given for a stored value.
   *
   * @param value the stored value
   * @return the value to write
   */
  @Nonnull
  public Column egress(@Nonnull final Column value) {
    return egress.apply(value);
  }
}
