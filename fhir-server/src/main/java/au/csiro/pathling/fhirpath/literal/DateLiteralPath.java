/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.literal;

import static au.csiro.pathling.utilities.Preconditions.check;
import static org.apache.spark.sql.functions.lit;

import au.csiro.pathling.fhirpath.Comparable;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.Materializable;
import au.csiro.pathling.fhirpath.element.DatePath;
import au.csiro.pathling.fhirpath.element.DateTimePath;
import java.text.ParseException;
import java.util.Date;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.DateType;
import org.hl7.fhir.r4.model.Type;

/**
 * Represents a FHIRPath date literal.
 *
 * @author John Grimes
 */
public class DateLiteralPath extends LiteralPath implements Materializable<DateType>, Comparable {

  @Nonnull
  private Optional<DateLiteralFormat> format;

  protected DateLiteralPath(@Nonnull final Dataset<Row> dataset, @Nonnull final Column idColumn,
      @Nonnull final Type literalValue) {
    super(dataset, idColumn, literalValue);
    check(literalValue instanceof DateType);
    format = Optional.empty();
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
    check(context.getIdColumn().isPresent());
    final String dateString = fhirPath.replaceFirst("^@", "");
    java.util.Date date;
    DateLiteralFormat format;
    // Try parsing out the date using the three possible formats, from full (most common) down to
    // the year only format.
    try {
      date = DatePath.getFullDateFormat().parse(dateString);
      format = DateLiteralFormat.FULL;
    } catch (final ParseException e) {
      try {
        date = DatePath.getYearMonthDateFormat().parse(dateString);
        format = DateLiteralFormat.YEAR_MONTH_DATE;
      } catch (final ParseException ex) {
        date = DatePath.getYearOnlyDateFormat().parse(dateString);
        format = DateLiteralFormat.YEAR_ONLY;
      }
    }

    final DateLiteralPath result = new DateLiteralPath(context.getDataset(),
        context.getIdColumn().get(), new DateType(date));
    result.format = Optional.of(format);
    return result;
  }

  @Nonnull
  @Override
  public String getExpression() {
    if (!format.isPresent() || format.get() == DateLiteralFormat.FULL) {
      return "@" + DatePath.getFullDateFormat().format(getLiteralValue().getValue());
    } else if (format.get() == DateLiteralFormat.YEAR_MONTH_DATE) {
      return "@" + DatePath.getYearMonthDateFormat().format(getLiteralValue().getValue());
    } else {
      return "@" + DatePath.getYearOnlyDateFormat().format(getLiteralValue().getValue());
    }
  }

  @Override
  public DateType getLiteralValue() {
    return (DateType) literalValue;
  }

  @Nonnull
  @Override
  public Date getJavaValue() {
    return getLiteralValue().getValue();
  }

  @Nonnull
  @Override
  public Column buildValueColumn() {
    final String date = DatePath.getFullDateFormat().format(getJavaValue());
    return lit(date);
  }

  @Override
  public Function<Comparable, Column> getComparison(final ComparisonOperation operation) {
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

  @Nonnull
  @Override
  public DateLiteralPath copy(@Nonnull final String expression,
      @Nonnull final Dataset<Row> dataset, @Nonnull final Optional<Column> idColumn,
      @Nonnull final Column valueColumn, final boolean singular,
      @Nonnull final Optional<Column> thisColumn) {
    check(idColumn.isPresent());
    return new DateLiteralPath(dataset, idColumn.get(), literalValue) {
      @Nonnull
      @Override
      public String getExpression() {
        return expression;
      }
    };
  }

  private enum DateLiteralFormat {
    FULL, YEAR_MONTH_DATE, YEAR_ONLY
  }

}
