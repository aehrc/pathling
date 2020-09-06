/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.literal;

import static au.csiro.pathling.utilities.Preconditions.check;

import au.csiro.pathling.fhirpath.Comparable;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.NonLiteralPath;
import au.csiro.pathling.fhirpath.Numeric;
import au.csiro.pathling.fhirpath.element.DecimalPath;
import au.csiro.pathling.fhirpath.element.IntegerPath;
import java.math.BigDecimal;
import java.util.function.Function;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.DecimalType;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Type;

/**
 * Represents a FHIRPath decimal literal.
 *
 * @author John Grimes
 */
public class DecimalLiteralPath extends LiteralPath implements Comparable, Numeric {

  @Nonnull
  private final DecimalType literalValue;

  @SuppressWarnings("WeakerAccess")
  protected DecimalLiteralPath(@Nonnull final Dataset<Row> dataset, @Nonnull final Column idColumn,
      @Nonnull final Type literalValue) {
    super(dataset, idColumn, literalValue);
    this.literalValue = (DecimalType) literalValue;
  }

  /**
   * Returns a new instance, parsed from a FHIRPath literal.
   *
   * @param fhirPath The FHIRPath representation of the literal
   * @param context An input context that can be used to build a {@link Dataset} to represent the
   * literal
   * @return A new instance of {@link LiteralPath}
   * @throws NumberFormatException if the literal is malformed
   */
  public static DecimalLiteralPath fromString(@Nonnull final String fhirPath,
      @Nonnull final FhirPath context) throws NumberFormatException {
    check(context.getIdColumn().isPresent());
    final BigDecimal value = new BigDecimal(fhirPath);
    return new DecimalLiteralPath(context.getDataset(), context.getIdColumn().get(),
        new DecimalType(value));
  }

  @Nonnull
  @Override
  public String getExpression() {
    return literalValue.getValue().toPlainString();
  }

  @Override
  @Nonnull
  public DecimalType getLiteralValue() {
    return literalValue;
  }

  @Nonnull
  @Override
  public BigDecimal getJavaValue() {
    return literalValue.getValue();
  }

  @Override
  public Function<Comparable, Column> getComparison(final ComparisonOperation operation) {
    return Comparable.buildComparison(this, operation.getSparkFunction());
  }

  @Override
  public boolean isComparableTo(@Nonnull final Class<? extends Comparable> type) {
    return IntegerPath.getComparableTypes().contains(type);
  }

  @Nonnull
  @Override
  public Function<Numeric, NonLiteralPath> getMathOperation(@Nonnull final MathOperation operation,
      @Nonnull final String expression, @Nonnull final Dataset<Row> dataset) {
    return DecimalPath
        .buildMathOperation(this, operation, expression, dataset, FHIRDefinedType.DECIMAL);
  }

}
