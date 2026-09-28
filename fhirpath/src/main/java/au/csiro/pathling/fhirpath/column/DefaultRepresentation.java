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

import au.csiro.pathling.encoders.ColumnFunctions;
import au.csiro.pathling.encoders.ValueFunctions;
import au.csiro.pathling.fhirpath.collection.DecimalCollection;
import au.csiro.pathling.fhirpath.collection.QuantityCollection;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
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
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
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

  /**
   * Traverses to a field of the structure, or of every structure in the array, that this
   * representation holds. The field is referenced through the tolerant traversal expression, so the
   * traversal yields a null of the null type where the input schema does not carry the field.
   *
   * @param fieldName the name of the field to traverse to
   * @return the flattened result of the traversal
   */
  @Nonnull
  @Override
  public ColumnRepresentation traverse(@Nonnull final String fieldName) {
    return traverse(fieldName, Optional.empty(), DataTypes.NullType);
  }

  @Override
  @Nonnull
  public ColumnRepresentation traverse(
      @Nonnull final String fieldName, @Nonnull final Optional<FHIRDefinedType> fhirType) {
    return traverse(fieldName, fhirType, AbsentElementTypes.singular(fhirType));
  }

  @Override
  @Nonnull
  public ColumnRepresentation traverse(
      @Nonnull final String fieldName,
      @Nonnull final Optional<FHIRDefinedType> fhirType,
      @Nonnull final DataType fallback) {
    final ColumnRepresentation result =
        copyOf(ColumnFunctions.resolveOrNull(value, fieldName, fallback)).removeNulls().flatten();
    @Nullable final FHIRDefinedType resolvedFhirType = fhirType.orElse(null);
    if (FHIRDefinedType.BASE64BINARY.equals(resolvedFhirType)) {
      // If the field is a base64Binary, represent it using a BinaryRepresentation.
      return DefaultRepresentation.fromBinaryColumn(result.getValue());
    } else if (FHIRDefinedType.DECIMAL.equals(resolvedFhirType)) {
      // A decimal is traversed to as text on either layout, and decoded for computation.
      return ElementRepresentation.ofPrimitive(
          DecimalCollection.decode(result), fhirType, this, fieldName);
    } else if (resolvedFhirType != null
        && QuantityCollection.QUANTITY_TYPES.contains(resolvedFhirType)) {
      // A quantity, of any of the quantity types, is traversed to in the new layout's shape on
      // either layout, and decoded for computation, which computes its canonical form (decision
      // 80).
      return QuantityCollection.decode(result);
    } else {
      // Otherwise, use the default representation, retaining the parent of a primitive.
      return ElementRepresentation.ofPrimitive(result, fhirType, this, fieldName);
    }
  }

  /**
   * Gets a field of the structure, or of every structure in the array, that this representation
   * holds, without flattening. The field is referenced through the tolerant traversal expression,
   * so the result is a null of the null type where the input schema does not carry the field.
   *
   * @param fieldName the name of the field to get
   * @return the field, unflattened
   */
  @Override
  @Nonnull
  public ColumnRepresentation getField(@Nonnull final String fieldName) {
    return copyOf(ColumnFunctions.resolveOrNull(value, fieldName, DataTypes.NullType));
  }
}
