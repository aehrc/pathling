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

import jakarta.annotation.Nonnull;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.types.DataType;

/**
 * How the values of one FHIR primitive type are stored, and how they are returned to JSON.
 *
 * <p>A converter states the types JSON inference may give its values, the type it stores them as,
 * and the two conversions. The acceptance check is per column rather than per value: inference
 * types a column from every value in a file, so a column whose type is not accepted carries at
 * least one value the definitions contradict, and the whole column is dropped rather than cast
 * value by value, which would turn {@code 1.5} into {@code 1} and {@code "yes"} into {@code true}
 * (decision 68).
 */
public final class PrimitiveConverter {

  @Nonnull private final Predicate<DataType> accepted;

  @Nonnull private final DataType stored;

  @Nonnull private final UnaryOperator<Column> ingest;

  @Nonnull private final UnaryOperator<Column> egress;

  private PrimitiveConverter(
      @Nonnull final Predicate<DataType> accepted,
      @Nonnull final DataType stored,
      @Nonnull final UnaryOperator<Column> ingest,
      @Nonnull final UnaryOperator<Column> egress) {
    this.accepted = accepted;
    this.stored = stored;
    this.ingest = ingest;
    this.egress = egress;
  }

  /**
   * Returns a converter with conversions of its own.
   *
   * @param accepted whether inference may give the values a type
   * @param stored the type the values are stored as
   * @param ingest the conversion of an accepted value to the stored type
   * @param egress the conversion of a stored value to what the JSON writer is given
   * @return the converter
   */
  @Nonnull
  public static PrimitiveConverter of(
      @Nonnull final Predicate<DataType> accepted,
      @Nonnull final DataType stored,
      @Nonnull final UnaryOperator<Column> ingest,
      @Nonnull final UnaryOperator<Column> egress) {
    return new PrimitiveConverter(accepted, stored, ingest, egress);
  }

  /**
   * Returns a converter that casts an accepted value to the stored type, and gives the JSON writer
   * the stored value as it is.
   *
   * <p>The cast is a {@code try_cast}, so a value the stored type cannot represent becomes null
   * rather than raising or wrapping: inference never produces an integer column, so an integer
   * element always arrives as a long, and one beyond the range of an integer would otherwise raise.
   *
   * @param stored the type the values are stored as
   * @param accepted the types inference may give the values
   * @return the converter
   */
  @Nonnull
  public static PrimitiveConverter casting(
      @Nonnull final DataType stored, @Nonnull final DataType... accepted) {
    return new PrimitiveConverter(
        Set.of(accepted)::contains,
        stored,
        value -> value.try_cast(stored),
        UnaryOperator.identity());
  }

  /**
   * Returns whether a column inferred as the given type can be stored by this converter.
   *
   * @param observed the type inference gave the column
   * @return true where the column is accepted
   */
  public boolean accepts(@Nonnull final DataType observed) {
    return accepted.test(observed);
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
   * Returns the stored value of an accepted value.
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
