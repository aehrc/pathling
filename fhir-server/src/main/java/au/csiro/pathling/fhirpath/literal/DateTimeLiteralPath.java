/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.literal;

import static au.csiro.pathling.utilities.Preconditions.check;

import au.csiro.pathling.fhirpath.Comparable;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.element.DateTimePath;
import java.text.ParseException;
import java.util.function.Function;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.DateTimeType;
import org.hl7.fhir.r4.model.Type;

/**
 * Represents a FHIRPath date literal.
 *
 * @author John Grimes
 */
public class DateTimeLiteralPath extends LiteralPath implements Comparable {

  @Nonnull
  private final DateTimeType literalValue;

  @SuppressWarnings("WeakerAccess")
  protected DateTimeLiteralPath(@Nonnull final Dataset<Row> dataset, @Nonnull final Column idColumn,
      @Nonnull final Type literalValue) {
    super(dataset, idColumn, literalValue);
    this.literalValue = (DateTimeType) literalValue;
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
  public static DateTimeLiteralPath fromString(@Nonnull final String fhirPath,
      @Nonnull final FhirPath context) throws ParseException {
    check(context.getIdColumn().isPresent());
    final String dateTimeString = fhirPath.replaceFirst("^@", "");
    final java.util.Date date = DateTimePath.getDateFormat().parse(dateTimeString);
    return new DateTimeLiteralPath(context.getDataset(), context.getIdColumn().get(),
        new DateTimeType(date));
  }

  @Nonnull
  @Override
  public String getExpression() {
    return "@" + DateTimePath.getDateFormat().format(literalValue.getValue());
  }

  @Nonnull
  @Override
  public java.sql.Timestamp getJavaValue() {
    return new java.sql.Timestamp(literalValue.getValue().getTime());
  }

  @Override
  public Function<Comparable, Column> getComparison(final ComparisonOperation operation) {
    return DateTimePath.buildComparison(this, operation.getSparkFunction());
  }

  @Override
  public boolean isComparableTo(@Nonnull final Class<? extends Comparable> type) {
    return DateTimePath.getComparableTypes().contains(type);
  }
}
