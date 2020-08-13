/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.element;

import static org.apache.spark.sql.functions.to_timestamp;

import au.csiro.pathling.fhirpath.Comparable;
import au.csiro.pathling.fhirpath.Materializable;
import java.text.SimpleDateFormat;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import javax.annotation.Nonnull;
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
public class DatePath extends ElementPath implements Materializable<DateType>, Comparable {

  private static final SimpleDateFormat FULL_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
  private static final SimpleDateFormat YEAR_MONTH_DATE_FORMAT = new SimpleDateFormat("yyyy-MM");
  private static final SimpleDateFormat YEAR_ONLY_DATE_FORMAT = new SimpleDateFormat("yyyy");

  static {
    FULL_DATE_FORMAT.setTimeZone(DateTimePath.getTimeZone());
    YEAR_MONTH_DATE_FORMAT.setTimeZone(DateTimePath.getTimeZone());
    YEAR_ONLY_DATE_FORMAT.setTimeZone(DateTimePath.getTimeZone());
  }

  /**
   * @param expression The FHIRPath representation of this path
   * @param dataset A {@link Dataset} that can be used to evaluate this path against data
   * @param idColumn A {@link Column} within the dataset containing the identity of the subject
   * resource
   * @param valueColumn A {@link Column} within the dataset containing the values of the nodes
   * @param singular An indicator of whether this path represents a single-valued collection
   * @param fhirType The FHIR datatype for this path, note that there can be more than one FHIR
   * type
   */
  public DatePath(@Nonnull final String expression, @Nonnull final Dataset<Row> dataset,
      @Nonnull final Optional<Column> idColumn, @Nonnull final Column valueColumn,
      final boolean singular, @Nonnull final FHIRDefinedType fhirType) {
    super(expression, dataset, idColumn, valueColumn, singular, fhirType);
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
    return FULL_DATE_FORMAT;
  }

  public static SimpleDateFormat getYearMonthDateFormat() {
    return YEAR_MONTH_DATE_FORMAT;
  }

  public static SimpleDateFormat getYearOnlyDateFormat() {
    return YEAR_ONLY_DATE_FORMAT;
  }

  @Nonnull
  @Override
  public Optional<DateType> getValueFromRow(@Nonnull final Row row, final int columnNumber) {
    if (row.isNullAt(columnNumber)) {
      return Optional.empty();
    }
    return Optional.of(new DateType(row.getString(columnNumber)));
  }

  @Override
  public Function<Comparable, Column> getComparison(
      final BiFunction<Column, Column, Column> sparkFunction) {
    return DateTimePath.buildComparison(this, sparkFunction);
  }

  @Override
  public boolean isComparableTo(@Nonnull final Class<? extends Comparable> type) {
    return DateTimePath.getComparableTypes().contains(type);
  }

}
