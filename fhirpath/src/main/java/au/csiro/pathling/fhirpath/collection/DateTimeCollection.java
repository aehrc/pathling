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
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import au.csiro.pathling.fhirpath.comparison.DateTimeSqlComparator;
import au.csiro.pathling.fhirpath.definition.NodeDefinition;
import au.csiro.pathling.sql.dates.datetime.DateTimeAddDurationFunction;
import au.csiro.pathling.sql.dates.datetime.DateTimeSubtractDurationFunction;
import com.google.common.collect.ImmutableSet;
import java.text.ParseException;
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

  private static final String SPARK_FHIRPATH_DATETIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";
  private static final ImmutableSet<Class<? extends Comparable>> COMPARABLE_TYPES = ImmutableSet
      .of(DateCollection.class, DateTimeCollection.class);

  protected DateTimeCollection(@Nonnull final ColumnRepresentation columnRepresentation,
      @Nonnull final Optional<FhirPathType> type,
      @Nonnull final Optional<FHIRDefinedType> fhirType,
      @Nonnull final Optional<? extends NodeDefinition> definition) {
    super(columnRepresentation, type, fhirType, definition);
  }

  /**
   * Returns a new instance with the specified column representation and definition.
   *
   * @param columnRepresentation The column representation to use
   * @param definition The definition to use
   * @return A new instance of {@link DateTimeCollection}
   */
  @Nonnull
  public static DateTimeCollection build(@Nonnull final ColumnRepresentation columnRepresentation,
      @Nonnull final Optional<NodeDefinition> definition) {
    return new DateTimeCollection(columnRepresentation, Optional.of(FhirPathType.DATETIME),
        Optional.of(FHIRDefinedType.DATETIME), definition);
  }

  /**
   * Returns a new instance with the specified column representation and no definition.
   *
   * @param columnRepresentation The column representation to use
   * @return A new instance of {@link DateTimeCollection}
   */
  @Nonnull
  public static DateTimeCollection build(@Nonnull final ColumnRepresentation columnRepresentation) {
    return DateTimeCollection.build(columnRepresentation, Optional.empty());
  }

  /**
   * Returns a new instance, parsed from a FHIRPath literal.
   *
   * @param fhirPath The FHIRPath representation of the literal
   * @return A new instance of {@link DateTimeCollection}
   * @throws ParseException if the literal is malformed
   */
  @Nonnull
  public static DateTimeCollection fromLiteral(@Nonnull final String fhirPath)
      throws ParseException {
    final String dateString = fhirPath.replaceFirst("^@", "");
    return DateTimeCollection.build(DefaultRepresentation.literal(dateString));
  }

  /**
   * Returns a new instance based upon a {@link DateTimeType}.
   *
   * @param value The value to use
   * @return A new instance of {@link DateTimeCollection}
   */
  @Nonnull
  public static DateTimeCollection fromValue(@Nonnull final DateTimeType value) {
    return DateTimeCollection.build(
        DefaultRepresentation.literal(value.getValueAsString()));
  }

  /**
   * Returns a new instance based upon a {@link InstantType}.
   *
   * @param value The value to use
   * @return A new instance of {@link DateTimeCollection}
   */
  @Nonnull
  public static DateTimeCollection fromValue(@Nonnull final InstantType value) {
    return DateTimeCollection.build(
        DefaultRepresentation.literal(value.getValueAsString()));
  }

  /**
   * Returns a set of classes that this collection can be compared to.
   *
   * @return A set of classes that this collection can be compared to
   */
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
  public StringCollection asStringPath() {
    final ColumnRepresentation valueColumn;
    if (getFhirType().isPresent() && getFhirType().get() == FHIRDefinedType.INSTANT) {
      valueColumn = getColumn().call(
          c -> date_format(c, SPARK_FHIRPATH_DATETIME_FORMAT));
    } else {
      valueColumn = getColumn();
    }
    return StringCollection.build(valueColumn);
  }

}
