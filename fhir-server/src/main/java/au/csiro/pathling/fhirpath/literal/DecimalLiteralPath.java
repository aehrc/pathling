/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.literal;

import static au.csiro.pathling.utilities.Preconditions.check;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.Comparable;
import au.csiro.pathling.fhirpath.*;
import au.csiro.pathling.fhirpath.element.DecimalPath;
import au.csiro.pathling.fhirpath.element.IntegerPath;
import java.math.BigDecimal;
import java.util.Optional;
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
public class DecimalLiteralPath extends LiteralPath implements Materializable<DecimalType>,
    Comparable, Numeric {

  @SuppressWarnings("WeakerAccess")
  protected DecimalLiteralPath(@Nonnull final Dataset<Row> dataset,
      @Nonnull final Optional<Column> idColumn, @Nonnull final Type literalValue) {
    super(dataset, idColumn, literalValue);
    check(literalValue instanceof DecimalType);
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
    final BigDecimal value = new BigDecimal(fhirPath);

    if (value.precision() > DecimalPath.getDecimalType().precision()) {
      throw new InvalidUserInputError(
          "Decimal literal exceeded maximum precision supported (" + DecimalPath.getDecimalType()
              .precision() + "): " + fhirPath);
    }
    if (value.scale() > DecimalPath.getDecimalType().scale()) {
      throw new InvalidUserInputError(
          "Decimal literal exceeded maximum scale supported (" + DecimalPath.getDecimalType()
              .scale() + "): " + fhirPath);
    }

    return new DecimalLiteralPath(context.getDataset(), context.getIdColumn(),
        new DecimalType(value));
  }

  @Nonnull
  @Override
  public String getExpression() {
    return getLiteralValue().getValue().toPlainString();
  }

  @Override
  @Nonnull
  public DecimalType getLiteralValue() {
    return (DecimalType) literalValue;
  }

  @Nonnull
  @Override
  public BigDecimal getJavaValue() {
    return getLiteralValue().getValue();
  }

  @Override
  @Nonnull
  public Function<Comparable, Column> getComparison(@Nonnull final ComparisonOperation operation) {
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

  @Nonnull
  @Override
  public Optional<DecimalType> getValueFromRow(@Nonnull final Row row, final int columnNumber) {
    return DecimalPath.valueFromRow(row, columnNumber);
  }

}
