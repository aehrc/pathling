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
   * traversal yields a null of the given type where the input schema does not carry the field.
   *
   * @param fieldName the name of the field to traverse to
   * @param fhirType the FHIR type of the field
   * @param fallback the type of the null that stands for the field where it is absent
   * @return the flattened result of the traversal
   */
  @Override
  @Nonnull
  public ColumnRepresentation traverse(
      @Nonnull final String fieldName,
      @Nonnull final Optional<FHIRDefinedType> fhirType,
      @Nonnull final DataType fallback) {
    return decodeField(
        copyOf(ColumnFunctions.resolveOrNull(value, fieldName, fallback)).removeNulls().flatten(),
        fhirType,
        this,
        fieldName);
  }

  /**
   * Decodes a traversed field for computation, by its FHIR type. The field has been read in the
   * shape the new layout gives it, on either layout.
   *
   * @param field the traversed field
   * @param fhirType the FHIR type of the field
   * @param parent the representation the field was traversed from
   * @param fieldName the name of the field
   * @return the decoded field
   */
  @Nonnull
  static ColumnRepresentation decodeField(
      @Nonnull final ColumnRepresentation field,
      @Nonnull final Optional<FHIRDefinedType> fhirType,
      @Nonnull final ColumnRepresentation parent,
      @Nonnull final String fieldName) {
    @Nullable final FHIRDefinedType resolvedFhirType = fhirType.orElse(null);
    if (FHIRDefinedType.BASE64BINARY.equals(resolvedFhirType)) {
      // If the field is a base64Binary, represent it using a BinaryRepresentation.
      return DefaultRepresentation.fromBinaryColumn(field.getValue());
    } else if (FHIRDefinedType.DECIMAL.equals(resolvedFhirType)) {
      // A decimal is read as text on either layout, and decoded for computation.
      return ElementRepresentation.ofPrimitive(
          DecimalCollection.decode(field), fhirType, parent, fieldName);
    } else if (resolvedFhirType != null
        && QuantityCollection.QUANTITY_TYPES.contains(resolvedFhirType)) {
      // A quantity, of any of the quantity types, is decoded for computation, which computes its
      // canonical form (decision 80).
      return QuantityCollection.decode(field);
    } else {
      // Otherwise, use the default representation, retaining the parent of a primitive.
      return ElementRepresentation.ofPrimitive(field, fhirType, parent, fieldName);
    }
  }

  /**
   * Gets a field of the structure, or of every structure in the array, that this representation
   * holds, without flattening. The field is referenced through the tolerant traversal expression,
   * so the result is a null of the given type where the input schema does not carry the field.
   *
   * @param fieldName the name of the field to get
   * @param fallback the type of the null that stands for the field where it is absent
   * @return the field, unflattened
   */
  @Override
  @Nonnull
  public ColumnRepresentation getField(
      @Nonnull final String fieldName, @Nonnull final DataType fallback) {
    return copyOf(ColumnFunctions.resolveOrNull(value, fieldName, fallback));
  }
}
