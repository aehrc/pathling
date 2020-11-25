/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.element;

import static org.apache.spark.sql.functions.to_timestamp;

import au.csiro.pathling.fhirpath.Comparable;
import au.csiro.pathling.fhirpath.Materializable;
import au.csiro.pathling.fhirpath.ResourcePath;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.DateType;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Represents a FHIRPath expression which refers to a date typed element.
 *
 * @author John Grimes
 */
@Slf4j
public class DatePath extends ElementPath implements Materializable<DateType>, Comparable {

  private static final ThreadLocal<SimpleDateFormat> FULL_DATE_FORMAT = ThreadLocal
      .withInitial(() -> {
        final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        format.setTimeZone(DateTimePath.getTimeZone());
        return format;
      });
  private static final ThreadLocal<SimpleDateFormat> YEAR_MONTH_DATE_FORMAT = ThreadLocal
      .withInitial(() -> {
        final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM");
        format.setTimeZone(DateTimePath.getTimeZone());
        return format;
      });
  private static final ThreadLocal<SimpleDateFormat> YEAR_ONLY_DATE_FORMAT = ThreadLocal
      .withInitial(() -> {
        final SimpleDateFormat format = new SimpleDateFormat("yyyy");
        format.setTimeZone(DateTimePath.getTimeZone());
        return format;
      });

  protected DatePath(@Nonnull final String expression, @Nonnull final Dataset<Row> dataset,
      @Nonnull final Column idColumn, @Nonnull final Optional<Column> eidColumn,
      @Nonnull final Column valueColumn,
      final boolean singular, @Nonnull final Optional<ResourcePath> foreignResource,
      @Nonnull final Optional<Column> thisColumn, @Nonnull final FHIRDefinedType fhirType) {
    super(expression, dataset, idColumn, eidColumn, valueColumn, singular, foreignResource,
        thisColumn, fhirType);
  }

  /**
   * Builds a comparison function for date and date/time like paths.
   *
   * @param source The path to build the comparison function for
   * @param sparkFunction The Spark column function to use
   * @return A new {@link Function}
   */
  @Nonnull
  public static Function<Comparable, Column> buildComparison(@Nonnull final Comparable source,
      @Nonnull final BiFunction<Column, Column, Column> sparkFunction) {
    // The value columns are converted to native Spark timestamps before comparison.
    return target -> sparkFunction
        .apply(org.apache.spark.sql.functions.to_date(source.getValueColumn()),
            to_timestamp(target.getValueColumn()));
  }

  public static SimpleDateFormat getFullDateFormat() {
    return FULL_DATE_FORMAT.get();
  }

  public static SimpleDateFormat getYearMonthDateFormat() {
    return YEAR_MONTH_DATE_FORMAT.get();
  }

  public static SimpleDateFormat getYearOnlyDateFormat() {
    return YEAR_ONLY_DATE_FORMAT.get();
  }

  @Nonnull
  @Override
  public Optional<DateType> getValueFromRow(@Nonnull final Row row, final int columnNumber) {
    return valueFromRow(row, columnNumber);
  }

  /**
   * Gets a value from a row for a Date or Date literal.
   *
   * @param row The {@link Row} from which to extract the value
   * @param columnNumber The column number to extract the value from
   * @return A {@link DateType}, or the absence of a value
   */
  @Nonnull
  public static Optional<DateType> valueFromRow(@Nonnull final Row row, final int columnNumber) {
    if (row.isNullAt(columnNumber)) {
      return Optional.empty();
    }
    final Date date;
    try {
      date = getFullDateFormat().parse(row.getString(columnNumber));
    } catch (final ParseException e) {
      log.warn("Error parsing date extracted from row", e);
      return Optional.empty();
    }
    return Optional.of(new DateType(date));
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

}
