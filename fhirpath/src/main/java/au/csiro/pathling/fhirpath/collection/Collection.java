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

package au.csiro.pathling.fhirpath.collection;

import au.csiro.pathling.fhirpath.FhirPathType;
import au.csiro.pathling.fhirpath.operator.comparison.ColumnComparator;
import au.csiro.pathling.fhirpath.operator.comparison.DefaultComparator;
import java.util.Optional;
import java.util.function.UnaryOperator;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.jetbrains.annotations.NotNull;

/**
 * Represents a collection of nodes that are the result of evaluating a FHIRPath expression.
 *
 * @author John Grimes
 */
public class Collection {

  private final @NotNull Column column;
  private final @NotNull Optional<FhirPathType> type;

  public Collection(final @NotNull Column column, final @NotNull Optional<FhirPathType> type) {
    this.column = column;
    this.type = type;
  }

  public @NotNull Collection traverse(final @NotNull String elementName) {
    if (elementName == null) {
      throw new IllegalArgumentException("Element name must not be null");
    }
    final Column newColumn = column.getField(elementName);
    return new Collection(newColumn, Optional.empty());
  }

  public @NotNull Collection map(final @NotNull UnaryOperator<Column> mapper) {
    return new Collection(mapper.apply(column), type);
  }

  public @NotNull ColumnComparator compare() {
    return new DefaultComparator();
  }

  public @NotNull Column getColumn() {
    return column;
  }

  public @NotNull Optional<FhirPathType> getType() {
    return type;
  }

  public @NotNull Collection singleton() {
    return this.map(
        c -> {
          final Expression columnExpression = c.expr();
          if (columnExpression.resolved()) {
            // If the column is resolved, we can inspect the data type.
            final DataType dataType = columnExpression.dataType();
            if (dataType instanceof org.apache.spark.sql.types.ArrayType) {
              // If the column is an array, we can check its size and enforce the rules here:
              // https://hl7.org/fhirpath/N1/#singleton-evaluation-of-collections
              return functions.when(functions.size(c).leq(1), functions.get(c, functions.lit(0)))
                  .otherwise(functions.raise_error(functions.lit("Expected a singular input")));
            }
          }
          // If the column is not resolved, assume that it is already a singular value.
          return c;
        });
  }

}
