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
import java.util.Optional;
import java.util.function.UnaryOperator;
import lombok.Getter;
import org.apache.spark.sql.Column;
import org.jetbrains.annotations.NotNull;

/**
 * Represents a collection of nodes that are the result of evaluating a FHIRPath expression.
 *
 * @author John Grimes
 */
@Getter
public class Collection {

  Column column;
  Optional<FhirPathType> type;

  public Collection(@NotNull final Column column, @NotNull final Optional<FhirPathType> type) {
    this.column = column;
    this.type = type;
  }

  @NotNull
  public Collection traverse(@NotNull final String elementName) {
    if (elementName == null) {
      throw new IllegalArgumentException("Element name must not be null");
    }
    final Column newColumn = column.getField(elementName);
    return new Collection(newColumn, type);
  }

  @NotNull
  public Collection map(@NotNull final UnaryOperator<Column> mapper) {
    return new Collection(mapper.apply(column), type);
  }

  @NotNull
  public Collection convert(@NotNull final FhirPathType newType)
      throws UnsupportedOperationException {
    throw new UnsupportedOperationException("Conversion not supported to type: " + newType);
  }

  @NotNull
  public ColumnComparator compare(@NotNull final Collection target)
      throws UnsupportedOperationException {
    throw new UnsupportedOperationException("Comparison not supported");
  }

}
