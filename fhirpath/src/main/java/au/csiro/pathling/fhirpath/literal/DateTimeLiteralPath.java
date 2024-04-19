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

package au.csiro.pathling.fhirpath.literal;

import static au.csiro.pathling.fhirpath.Temporal.buildDateArithmeticOperation;
import static org.apache.spark.sql.functions.lit;

import au.csiro.pathling.fhirpath.Comparable;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.Materializable;
import au.csiro.pathling.fhirpath.Numeric.MathOperation;
import au.csiro.pathling.fhirpath.Temporal;
import au.csiro.pathling.fhirpath.comparison.DateTimeSqlComparator;
import au.csiro.pathling.fhirpath.element.DateTimePath;
import au.csiro.pathling.sql.dates.datetime.DateTimeAddDurationFunction;
import au.csiro.pathling.sql.dates.datetime.DateTimeSubtractDurationFunction;
import jakarta.annotation.Nonnull;
import java.text.ParseException;
import java.util.Optional;
import java.util.function.Function;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.BaseDateTimeType;
import org.hl7.fhir.r4.model.DateTimeType;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Represents a FHIRPath date literal.
 *
 * @author John Grimes
 */
public class DateTimeLiteralPath extends LiteralPath<BaseDateTimeType> implements
    Materializable<BaseDateTimeType>, Comparable, Temporal {

  protected DateTimeLiteralPath(@Nonnull final Dataset<Row> dataset, @Nonnull final Column idColumn,
      @Nonnull final BaseDateTimeType literalValue) {
    super(dataset, idColumn, literalValue);
  }

  protected DateTimeLiteralPath(@Nonnull final Dataset<Row> dataset, @Nonnull final Column idColumn,
      @Nonnull final BaseDateTimeType literalValue, @Nonnull final String expression) {
    super(dataset, idColumn, literalValue, expression);
  }

  /**
   * Returns a new instance, parsed from a FHIRPath literal.
   *
   * @param fhirPath The FHIRPath representation of the literal
   * @param context An input context that can be used to build a {@link Dataset} to represent the
   * literal
   * @return A new instance of {@link LiteralPath}
   * @throws ParseException if the literal is malformed
   */
  @Nonnull
  public static DateTimeLiteralPath fromString(@Nonnull final String fhirPath,
      @Nonnull final FhirPath context) throws ParseException {
    final String dateTimeString = fhirPath.replaceFirst("^@", "");
    final DateTimeType dateTimeType = new DateTimeType(dateTimeString);
    return new DateTimeLiteralPath(context.getDataset(), context.getIdColumn(), dateTimeType,
        fhirPath);
  }

  @Nonnull
  @Override
  public String getExpression() {
    return expression.orElse("@" + getValue().getValueAsString());
  }

  @Nonnull
  @Override
  public Column buildValueColumn() {
    return lit(getValue().asStringValue());
  }

  @Override
  @Nonnull
  public Function<Comparable, Column> getComparison(@Nonnull final ComparisonOperation operation) {
    return DateTimeSqlComparator.buildComparison(this, operation);
  }

  @Override
  public boolean isComparableTo(@Nonnull final Class<? extends Comparable> type) {
    return DateTimePath.getComparableTypes().contains(type);
  }

  @Nonnull
  @Override
  public Optional<BaseDateTimeType> getValueFromRow(@Nonnull final Row row,
      final int columnNumber) {
    return DateTimePath.valueFromRow(row, columnNumber, FHIRDefinedType.DATETIME);
  }

  @Override
  public boolean canBeCombinedWith(@Nonnull final FhirPath target) {
    return super.canBeCombinedWith(target) || target instanceof DateTimePath;
  }

  @Nonnull
  @Override
  public Function<QuantityLiteralPath, FhirPath> getDateArithmeticOperation(
      @Nonnull final MathOperation operation, @Nonnull final Dataset<Row> dataset,
      @Nonnull final String expression) {
    return buildDateArithmeticOperation(this, operation, dataset, expression,
        DateTimeAddDurationFunction.FUNCTION_NAME, DateTimeSubtractDurationFunction.FUNCTION_NAME);
  }

}
