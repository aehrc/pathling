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

import au.csiro.pathling.fhirpath.FhirPathType;
import au.csiro.pathling.fhirpath.Materializable;
import au.csiro.pathling.fhirpath.StringCoercible;
import au.csiro.pathling.fhirpath.Temporal;
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import au.csiro.pathling.fhirpath.comparison.DateTimeComparator;
import au.csiro.pathling.fhirpath.definition.NodeDefinition;
import au.csiro.pathling.fhirpath.operator.Comparable;
import au.csiro.pathling.sql.dates.date.DateAddDurationFunction;
import au.csiro.pathling.sql.dates.date.DateSubtractDurationFunction;
import jakarta.annotation.Nonnull;
import java.text.ParseException;
import java.util.Optional;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
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

  protected DateCollection(@Nonnull final ColumnRepresentation columnRepresentation,
      @Nonnull final Optional<FhirPathType> type,
      @Nonnull final Optional<FHIRDefinedType> fhirType,
      @Nonnull final Optional<? extends NodeDefinition> definition) {
    super(columnRepresentation, type, fhirType, definition);
  }

  /**
   * Returns a new instance with the specified columnCtx and definition.
   *
   * @param columnRepresentation The columnCtx to use
   * @param definition The definition to use
   * @return A new instance of {@link DateCollection}
   */
  @Nonnull
  public static DateCollection build(@Nonnull final ColumnRepresentation columnRepresentation,
      @Nonnull final Optional<NodeDefinition> definition) {
    return new DateCollection(columnRepresentation, Optional.of(FhirPathType.DATE),
        Optional.of(FHIRDefinedType.DATE), definition);
  }

  @Nonnull
  private static DateCollection build(@Nonnull final ColumnRepresentation columnRepresentation) {
    return DateCollection.build(columnRepresentation, Optional.empty());
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
    return DateCollection.build(DefaultRepresentation.literal(dateString));
  }

  /**
   * Returns a new instance based upon a {@link DateType}.
   *
   * @param value The value to use
   * @return A new instance of {@link DateCollection}
   */
  @Nonnull
  public static DateCollection fromValue(@Nonnull final DateType value) {
    return DateCollection.build(DefaultRepresentation.literal(value.getValueAsString()));
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
    return DateTimeComparator.buildComparison(this, operation);
  }

  @Override
  public boolean isComparableTo(@Nonnull final Collection path) {
    return DateTimeCollection.getComparableTypes().contains(path.getClass())
        || super.isComparableTo(path);
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
  public StringCollection asStringPath() {
    return map(ColumnRepresentation::asString, StringCollection::build);
  }

}
