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

package au.csiro.pathling.fhirpath.column;

import static org.apache.spark.sql.functions.lit;

import au.csiro.pathling.encoders.ValueFunctions;
import jakarta.annotation.Nonnull;
import java.math.BigDecimal;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.spark.sql.Column;
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

  Column value;

  /**
   * Create a new {@link ColumnRepresentation} from a literal value.
   *
   * @param value The value to represent
   * @return A new {@link ColumnRepresentation} representing the value
   */
  @Nonnull
  public static ColumnRepresentation literal(@Nonnull final Object value) {
    if (value instanceof BigDecimal) {
      // If the literal is a BigDecimal, represent it as a DecimalRepresentation.
      return new DecimalRepresentation(lit(value));
    }
    // Otherwise use the default representation.
    return new DefaultRepresentation(lit(value));
  }

  @Override
  protected ColumnRepresentation copyOf(@Nonnull final Column newValue) {
    return new DefaultRepresentation(newValue);
  }

  @Override
  @Nonnull
  public DefaultRepresentation vectorize(
      @Nonnull final Function<Column, Column> arrayExpression,
      @Nonnull final Function<Column, Column> singularExpression) {
    return new DefaultRepresentation(
        ValueFunctions.ifArray(value, arrayExpression::apply, singularExpression::apply));
  }

  @Override
  @Nonnull
  public DefaultRepresentation flatten() {
    return new DefaultRepresentation(ValueFunctions.unnest(value));
  }

  @Nonnull
  @Override
  public DefaultRepresentation traverse(@Nonnull final String fieldName) {
    return new DefaultRepresentation(traverseColumn(fieldName));
  }

  @Override
  @Nonnull
  public DefaultRepresentation traverse(@Nonnull final String fieldName,
      @Nonnull final Optional<FHIRDefinedType> fhirType) {
    @Nullable final FHIRDefinedType resolvedFhirType = fhirType.orElse(null);
    if (FHIRDefinedType.DECIMAL.equals(resolvedFhirType)) {
      // If the field is a decimal, represent it using a DecimalRepresentation.
      return DecimalRepresentation.fromTraversal(this, fieldName);
    } else if (FHIRDefinedType.BASE64BINARY.equals(resolvedFhirType)) {
      // If the field is a base64Binary, represent it using a BinaryRepresentation.
      return new BinaryRepresentation(traverseColumn(fieldName));
    } else {
      // Otherwise, use the default representation.
      return traverse(fieldName);
    }
  }

  /**
   * @param fieldName the name of the field to traverse
   * @return a new column representing the field
   */
  protected Column traverseColumn(@Nonnull final String fieldName) {
    return ValueFunctions.unnest(value.getField(fieldName));
  }

}
