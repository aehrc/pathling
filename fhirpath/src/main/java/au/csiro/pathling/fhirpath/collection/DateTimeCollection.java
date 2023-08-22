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
import static org.apache.spark.sql.functions.date_format;

import au.csiro.pathling.fhirpath.Comparable;
import au.csiro.pathling.fhirpath.FhirPathType;
import au.csiro.pathling.fhirpath.Materializable;
import au.csiro.pathling.fhirpath.StringCoercible;
import au.csiro.pathling.fhirpath.Temporal;
import au.csiro.pathling.fhirpath.comparison.DateTimeSqlComparator;
import au.csiro.pathling.fhirpath.definition.NodeDefinition;
import au.csiro.pathling.sql.dates.datetime.DateTimeAddDurationFunction;
import au.csiro.pathling.sql.dates.datetime.DateTimeSubtractDurationFunction;
import com.google.common.collect.ImmutableSet;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.BaseDateTimeType;
import org.hl7.fhir.r4.model.DateTimeType;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.InstantType;

/**
 * Represents a collection of DateTime-typed elements.
 *
 * @author John Grimes
 */
public class DateTimeCollection extends Collection implements
    Materializable<BaseDateTimeType>, Comparable, Temporal, StringCoercible {

  public static final String SPARK_FHIRPATH_DATETIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";
  private static final ImmutableSet<Class<? extends Comparable>> COMPARABLE_TYPES = ImmutableSet
      .of(DateCollection.class, DateTimeCollection.class);

  public DateTimeCollection(@Nonnull final Column column,
      @Nonnull final Optional<NodeDefinition> definition) {
    super(column, Optional.of(FhirPathType.DATETIME), Optional.of(FHIRDefinedType.DATETIME),
        definition);
  }

  @Nonnull
  public static DateTimeCollection build(@Nonnull final Column column,
      @Nonnull final Optional<NodeDefinition> definition) {
    return new DateTimeCollection(column, definition);
  }

  @Nonnull
  public static ImmutableSet<Class<? extends Comparable>> getComparableTypes() {
    return COMPARABLE_TYPES;
  }

  @Nonnull
  @Override
  public Optional<BaseDateTimeType> getFhirValueFromRow(@Nonnull final Row row,
      final int columnNumber) {
    if (row.isNullAt(columnNumber)) {
      return Optional.empty();
    }
    if (getFhirType().isPresent() && getFhirType().get() == FHIRDefinedType.INSTANT) {
      final InstantType value = new InstantType(row.getTimestamp(columnNumber));
      return Optional.of(value);
    } else {
      final DateTimeType value = new DateTimeType(row.getString(columnNumber));
      return Optional.of(value);
    }
  }

  @Override
  @Nonnull
  public Function<Comparable, Column> getComparison(@Nonnull final ComparisonOperation operation) {
    return DateTimeSqlComparator.buildComparison(this, operation);
  }

  @Override
  public boolean isComparableTo(@Nonnull final Collection path) {
    return COMPARABLE_TYPES.contains(path.getClass());
  }

  @Nonnull
  @Override
  public Function<QuantityCollection, Collection> getDateArithmeticOperation(
      @Nonnull final MathOperation operation) {
    return buildDateArithmeticOperation(this, operation,
        DateTimeAddDurationFunction.FUNCTION_NAME, DateTimeSubtractDurationFunction.FUNCTION_NAME);
  }

  @Nonnull
  @Override
  public Collection asStringPath() {
    final Column valueColumn;
    if (getFhirType().isPresent() && getFhirType().get() == FHIRDefinedType.INSTANT) {
      valueColumn = date_format(getColumn(), SPARK_FHIRPATH_DATETIME_FORMAT);
    } else {
      valueColumn = getColumn();
    }
    return StringCollection.build(valueColumn, Optional.empty());
  }

}
