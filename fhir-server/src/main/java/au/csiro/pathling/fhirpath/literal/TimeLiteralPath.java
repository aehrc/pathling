/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.literal;

import static org.apache.spark.sql.functions.lit;

import au.csiro.pathling.fhirpath.Comparable;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.Materializable;
import au.csiro.pathling.fhirpath.element.TimePath;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.TimeType;

/**
 * Represents a FHIRPath time literal.
 *
 * @author John Grimes
 */
public class TimeLiteralPath extends LiteralPath<TimeType> implements Materializable<TimeType>,
    Comparable {

  protected TimeLiteralPath(@Nonnull final Dataset<Row> dataset, @Nonnull final Column idColumn,
      @Nonnull final TimeType literalValue) {
    super(dataset, idColumn, literalValue);
  }

  protected TimeLiteralPath(@Nonnull final Dataset<Row> dataset, @Nonnull final Column idColumn,
      @Nonnull final TimeType literalValue, @Nonnull final String expression) {
    super(dataset, idColumn, literalValue, expression);
  }

  /**
   * Returns a new instance, parsed from a FHIRPath literal.
   *
   * @param fhirPath The FHIRPath representation of the literal
   * @param context An input context that can be used to build a {@link Dataset} to represent the
   * literal
   * @return A new instance of {@link LiteralPath}
   */
  @Nonnull
  public static TimeLiteralPath fromString(@Nonnull final String fhirPath,
      @Nonnull final FhirPath context) {
    final String timeString = fhirPath.replaceFirst("^@T", "");
    return new TimeLiteralPath(context.getDataset(), context.getIdColumn(),
        new TimeType(timeString), fhirPath);
  }

  @Nonnull
  @Override
  public String getExpression() {
    return expression.orElse("@T" + getValue().getValue());
  }

  @Nonnull
  @Override
  public Column buildValueColumn() {
    return lit(getValue().asStringValue());
  }

  @Override
  @Nonnull
  public Function<Comparable, Column> getComparison(@Nonnull final ComparisonOperation operation) {
    return Comparable.buildComparison(this, operation.getSparkFunction());
  }

  @Override
  public boolean isComparableTo(@Nonnull final Class<? extends Comparable> type) {
    return TimePath.getComparableTypes().contains(type);
  }

  @Nonnull
  @Override
  public Optional<TimeType> getValueFromRow(@Nonnull final Row row, final int columnNumber) {
    return TimePath.valueFromRow(row, columnNumber);
  }

  @Override
  public boolean canBeCombinedWith(@Nonnull final FhirPath target) {
    return super.canBeCombinedWith(target) || target instanceof TimePath;
  }

}
