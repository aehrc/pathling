/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.literal;

import au.csiro.pathling.fhirpath.Comparable;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.element.DatePath;
import au.csiro.pathling.fhirpath.element.DateTimePath;
import java.text.ParseException;
import java.util.function.BiFunction;
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
public class DateLiteralPath extends LiteralPath implements Comparable {

  @Nonnull
  private final DateType literalValue;

  @SuppressWarnings("WeakerAccess")
  protected DateLiteralPath(@Nonnull final Dataset<Row> dataset, @Nonnull final Column idColumn,
      @Nonnull final Type literalValue) {
    super(dataset, idColumn, literalValue);
    this.literalValue = (DateType) literalValue;
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
    java.util.Date date;
    // Try parsing out the date using the three possible formats, from full (most common) down to
    // the year only format.
    try {
      date = DatePath.getFullDateFormat().parse(dateString);
    } catch (final ParseException e) {
      try {
        date = DatePath.getYearMonthDateFormat().parse(dateString);
      } catch (final ParseException ex) {
        date = DatePath.getYearOnlyDateFormat().parse(dateString);
      }
    }
    return new DateLiteralPath(context.getDataset(), context.getIdColumn(), new DateType(date));
  }

  @Nonnull
  @Override
  public String getExpression() {
    // One the way back out, the date is always formatted using the "full" format, even if it was
    // created from one of the shorter formats.
    return "@" + DatePath.getFullDateFormat().format(literalValue.getValue());
  }

  @Nonnull
  @Override
  public java.sql.Date getJavaValue() {
    return new java.sql.Date(literalValue.getValue().getTime());
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
