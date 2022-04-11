/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.literal;

import static au.csiro.pathling.fhirpath.Temporal.buildDateArithmeticOperation;
import static org.apache.spark.sql.functions.lit;

import au.csiro.pathling.fhirpath.Comparable;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.Materializable;
import au.csiro.pathling.fhirpath.Numeric.MathOperation;
import au.csiro.pathling.fhirpath.Temporal;
import au.csiro.pathling.fhirpath.element.DatePath;
import au.csiro.pathling.fhirpath.element.DateTimePath;
import au.csiro.pathling.sql.dates.AddDurationToDate;
import au.csiro.pathling.sql.dates.SubtractDurationFromDate;
import java.text.ParseException;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.DateType;

/**
 * Represents a FHIRPath date literal.
 *
 * @author John Grimes
 */
public class DateLiteralPath extends LiteralPath<DateType> implements Materializable<DateType>,
    Comparable, Temporal {

  protected DateLiteralPath(@Nonnull final Dataset<Row> dataset, @Nonnull final Column idColumn,
      @Nonnull final DateType literalValue) {
    super(dataset, idColumn, literalValue);
  }

  protected DateLiteralPath(@Nonnull final Dataset<Row> dataset, @Nonnull final Column idColumn,
      @Nonnull final DateType literalValue, @Nonnull final String expression) {
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
  public static DateLiteralPath fromString(@Nonnull final String fhirPath,
      @Nonnull final FhirPath context) throws ParseException {
    final String dateString = fhirPath.replaceFirst("^@", "");
    final DateType dateType = new DateType(dateString);
    return new DateLiteralPath(context.getDataset(), context.getIdColumn(), dateType, fhirPath);
  }

  @Nonnull
  @Override
  public String getExpression() {
    return expression.orElse("@" + getValue().asStringValue());
  }

  @Nonnull
  @Override
  public Column buildValueColumn() {
    return lit(getValue().asStringValue());
  }

  @Override
  @Nonnull
  public Function<Comparable, Column> getComparison(@Nonnull final ComparisonOperation operation) {
    return DateTimePath.buildComparison(this, operation.getSparkFunction());
  }

  @Override
  public boolean isComparableTo(@Nonnull final Class<? extends Comparable> type) {
    return DateTimePath.getComparableTypes().contains(type);
  }

  @Nonnull
  @Override
  public Optional<DateType> getValueFromRow(@Nonnull final Row row, final int columnNumber) {
    return DatePath.valueFromRow(row, columnNumber);
  }

  @Override
  public boolean canBeCombinedWith(@Nonnull final FhirPath target) {
    return super.canBeCombinedWith(target) || target instanceof DatePath;
  }

  @Nonnull
  @Override
  public Function<QuantityLiteralPath, FhirPath> getDateArithmeticOperation(
      @Nonnull final MathOperation operation, @Nonnull final Dataset<Row> dataset,
      @Nonnull final String expression) {
    return buildDateArithmeticOperation(this, operation, dataset, expression,
        AddDurationToDate.FUNCTION_NAME, SubtractDurationFromDate.FUNCTION_NAME);
  }

}
