/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.literal;

import static au.csiro.pathling.utilities.Preconditions.check;

import au.csiro.pathling.fhirpath.Comparable;
import au.csiro.pathling.fhirpath.*;
import au.csiro.pathling.fhirpath.element.IntegerPath;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.IntegerType;
import org.hl7.fhir.r4.model.PrimitiveType;
import org.hl7.fhir.r4.model.Type;

/**
 * Represents a FHIRPath integer literal.
 *
 * @author John Grimes
 */
public class IntegerLiteralPath extends LiteralPath implements Materializable<PrimitiveType>,
    Comparable, Numeric {

  @SuppressWarnings("WeakerAccess")
  protected IntegerLiteralPath(@Nonnull final Dataset<Row> dataset, @Nonnull final Column idColumn,
      @Nonnull final Type literalValue) {
    super(dataset, idColumn, literalValue);
    check(literalValue instanceof IntegerType);
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
  public static IntegerLiteralPath fromString(@Nonnull final String fhirPath,
      @Nonnull final FhirPath context) throws NumberFormatException {
    final int value = Integer.parseInt(fhirPath);
    return new IntegerLiteralPath(context.getDataset(), context.getIdColumn(),
        new IntegerType(value));
  }

  @Nonnull
  @Override
  public String getExpression() {
    return getLiteralValue().getValueAsString();
  }

  @Override
  @Nonnull
  public IntegerType getLiteralValue() {
    return (IntegerType) literalValue;
  }

  @Nonnull
  @Override
  public Integer getJavaValue() {
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
    return IntegerPath
        .buildMathOperation(this, operation, expression, dataset, FHIRDefinedType.INTEGER);
  }

  @Nonnull
  @Override
  public Optional<PrimitiveType> getValueFromRow(@Nonnull final Row row, final int columnNumber) {
    return IntegerPath.valueFromRow(row, columnNumber, FHIRDefinedType.INTEGER);
  }

}
