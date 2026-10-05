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

package au.csiro.pathling.fhirpath.column;

import static org.apache.spark.sql.functions.lit;

import au.csiro.pathling.encoders.ValueFunctions;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import java.util.function.UnaryOperator;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Describes a representation where collections of values are represented as arrays in the dataset.
 *
 * @author Piotr Szul
 * @author John Grimes
 */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
@AllArgsConstructor
public class DefaultRepresentation extends ColumnRepresentation {

  private static final DefaultRepresentation EMPTY_REPRESENTATION =
      new DefaultRepresentation(functions.lit(null));

  /**
   * Gets the empty representation.
   *
   * @return A singleton instance of an empty representation
   */
  @Nonnull
  public static DefaultRepresentation empty() {
    return EMPTY_REPRESENTATION;
  }

  /** The column value represented by this object. */
  @Setter(AccessLevel.PROTECTED)
  @Nonnull
  private Column value;

  /**
   * Create a new {@link ColumnRepresentation} from a literal value.
   *
   * @param value The value to represent
   * @return A new {@link ColumnRepresentation} representing the value
   */
  @Nonnull
  public static ColumnRepresentation literal(@Nonnull final Object value) {
    if (value instanceof final byte[] ba) {
      return fromBinaryColumn(functions.lit(ba));
    } else {
      // Otherwise use the default representation.
      return new DefaultRepresentation(lit(value));
    }
  }

  /**
   * Creates a new {@link ColumnRepresentation} that represents a binary column as a base64 encoded
   * string.
   *
   * @param column a column containing binary data
   * @return A new {@link ColumnRepresentation} representing the binary data as a base64 encoded
   *     string.
   */
  @Nonnull
  public static ColumnRepresentation fromBinaryColumn(@Nonnull final Column column) {
    return new DefaultRepresentation(column).transform(functions::base64);
  }

  @Override
  @Nonnull
  public DefaultRepresentation copyOf(@Nonnull final Column newValue) {
    return new DefaultRepresentation(newValue);
  }

  @Override
  @Nonnull
  public DefaultRepresentation vectorize(
      @Nonnull final UnaryOperator<Column> arrayExpression,
      @Nonnull final UnaryOperator<Column> singularExpression) {
    return copyOf(ValueFunctions.ifArray(value, arrayExpression, singularExpression));
  }

  @Override
  @Nonnull
  public DefaultRepresentation flatten() {
    return copyOf(ValueFunctions.unnest(value));
  }

  @Nonnull
  @Override
  public ColumnRepresentation traverse(@Nonnull final String fieldName) {
    return fromField(ValueFunctions.nullSafeField(value, fieldName));
  }

  @Override
  @Nonnull
  public ColumnRepresentation traverse(
      @Nonnull final String fieldName, @Nonnull final Optional<FHIRDefinedType> fhirType) {
    return withFhirType(traverse(fieldName), fhirType);
  }

  @Override
  @Nonnull
  public ColumnRepresentation traverseOrEmpty(
      @Nonnull final String fieldName, @Nonnull final Optional<FHIRDefinedType> fhirType) {
    return withFhirType(fromField(ValueFunctions.fieldOrNull(value, fieldName)), fhirType);
  }

  @Override
  @Nonnull
  public ColumnRepresentation getField(@Nonnull final String fieldName) {
    return copyOf(ValueFunctions.nullSafeField(value, fieldName));
  }

  /**
   * Builds the representation of a traversed field, which is flattened and has null elements
   * removed.
   *
   * @param field The extracted field
   * @return The representation of the field
   */
  @Nonnull
  private ColumnRepresentation fromField(@Nonnull final Column field) {
    return copyOf(field).removeNulls().flatten();
  }

  /**
   * Adapts the representation of a traversed field to its FHIR type.
   *
   * @param field The representation of the traversed field
   * @param fhirType The FHIR type of the field
   * @return The representation of the field, adapted to its type
   */
  @Nonnull
  private static ColumnRepresentation withFhirType(
      @Nonnull final ColumnRepresentation field,
      @Nonnull final Optional<FHIRDefinedType> fhirType) {
    if (fhirType.filter(FHIRDefinedType.BASE64BINARY::equals).isPresent()) {
      // If the field is a base64Binary, represent it using a BinaryRepresentation.
      return DefaultRepresentation.fromBinaryColumn(field.getValue());
    } else {
      // Otherwise, use the default representation.
      return field;
    }
  }
}
