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

import au.csiro.pathling.encoders.ValueFunctions;
import java.math.BigDecimal;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
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
public class ArrayOrSingularRepresentation extends ColumnRepresentation {

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
      return new DecimalRepresentation(functions.lit(value));
    }
    return new ArrayOrSingularRepresentation(functions.lit(value));
  }

  @Override
  protected ColumnRepresentation copyOf(@Nonnull final Column newValue) {
    return new ArrayOrSingularRepresentation(newValue);
  }

  @Override
  @Nonnull
  public ArrayOrSingularRepresentation vectorize(
      @Nonnull final Function<Column, Column> arrayExpression,
      @Nonnull final Function<Column, Column> singularExpression) {
    return new ArrayOrSingularRepresentation(
        ValueFunctions.ifArray(value, arrayExpression::apply, singularExpression::apply));
  }

  @Override
  @Nonnull
  public ArrayOrSingularRepresentation flatten() {
    return new ArrayOrSingularRepresentation(ValueFunctions.unnest(value));
  }

  @Nonnull
  @Override
  public ArrayOrSingularRepresentation traverse(@Nonnull final String fieldName) {
    return new ArrayOrSingularRepresentation(traverseColumn(fieldName));
  }

  @Override
  @Nonnull
  public ArrayOrSingularRepresentation traverse(@Nonnull final String fieldName,
      @Nonnull final Optional<FHIRDefinedType> fhirType) {
    @Nullable final FHIRDefinedType resolvedFhirType = fhirType.orElse(null);
    if (FHIRDefinedType.DECIMAL.equals(resolvedFhirType)) {
      return DecimalRepresentation.fromTraversal(this, fieldName);
    } else if (FHIRDefinedType.BASE64BINARY.equals(resolvedFhirType)) {
      return new BinaryRepresentation(traverseColumn(fieldName));
    } else {
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
