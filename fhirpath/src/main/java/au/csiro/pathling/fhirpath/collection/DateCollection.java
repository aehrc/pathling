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

import static au.csiro.pathling.fhirpath.Temporal.buildDateArithmeticOperation;
import static org.apache.spark.sql.functions.lit;

import au.csiro.pathling.fhirpath.Comparable;
import au.csiro.pathling.fhirpath.FhirPathType;
import au.csiro.pathling.fhirpath.Materializable;
import au.csiro.pathling.fhirpath.StringCoercible;
import au.csiro.pathling.fhirpath.Temporal;
import au.csiro.pathling.fhirpath.comparison.DateTimeSqlComparator;
import au.csiro.pathling.fhirpath.definition.NodeDefinition;
import au.csiro.pathling.sql.dates.date.DateAddDurationFunction;
import au.csiro.pathling.sql.dates.date.DateSubtractDurationFunction;
import java.text.ParseException;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.DateType;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Represents a collection of Date-typed elements.
 *
 * @author John Grimes
 */
@Slf4j
public class DateCollection extends Collection implements Materializable<DateType>,
    Comparable, Temporal, StringCoercible {

  public DateCollection(@Nonnull final Column column,
      @Nonnull final Optional<NodeDefinition> definition) {
    super(column, Optional.of(FhirPathType.DATE), Optional.of(FHIRDefinedType.DATE), definition);
  }

  @Nonnull
  public static DateCollection build(@Nonnull final Column column,
      @Nonnull final Optional<NodeDefinition> definition) {
    return new DateCollection(column, definition);
  }

  /**
   * Returns a new instance, parsed from a FHIRPath literal.
   *
   * @param fhirPath The FHIRPath representation of the literal
   * @return A new instance of {@link DateCollection}
   * @throws ParseException if the literal is malformed
   */
  @Nonnull
  public static DateCollection fromLiteral(@Nonnull final String fhirPath) throws ParseException {
    final String dateString = fhirPath.replaceFirst("^@", "");
    return DateCollection.build(lit(dateString), Optional.empty());
  }

  @Nonnull
  @Override
  public Optional<DateType> getFhirValueFromRow(@Nonnull final Row row, final int columnNumber) {
    if (row.isNullAt(columnNumber)) {
      return Optional.empty();
    }
    final String dateString = row.getString(columnNumber);
    return Optional.of(new DateType(dateString));
  }

  @Override
  @Nonnull
  public Function<Comparable, Column> getComparison(@Nonnull final ComparisonOperation operation) {
    return DateTimeSqlComparator.buildComparison(this, operation);
  }

  @Override
  public boolean isComparableTo(@Nonnull final Collection path) {
    return DateTimeCollection.getComparableTypes().contains(path.getClass());
  }

  @Nonnull
  @Override
  public Function<QuantityCollection, Collection> getDateArithmeticOperation(
      @Nonnull final MathOperation operation) {
    return buildDateArithmeticOperation(this, operation,
        DateAddDurationFunction.FUNCTION_NAME, DateSubtractDurationFunction.FUNCTION_NAME);
  }

  @Nonnull
  @Override
  public Collection asStringPath() {
    return StringCollection.build(getColumn().cast(DataTypes.StringType), Optional.empty());
  }

}
