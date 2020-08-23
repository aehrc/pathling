/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.literal;

import static au.csiro.pathling.utilities.Preconditions.check;

import au.csiro.pathling.fhirpath.Comparable;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.element.BooleanPath;
import java.util.function.Function;
import javax.annotation.Nonnull;
import lombok.Getter;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.Type;

/**
 * Represents a FHIRPath boolean literal.
 *
 * @author John Grimes
 */
public class BooleanLiteralPath extends LiteralPath implements Comparable {

  @Nonnull
  @Getter
  private final BooleanType literalValue;

  @SuppressWarnings("WeakerAccess")
  protected BooleanLiteralPath(@Nonnull final Dataset<Row> dataset, @Nonnull final Column idColumn,
      @Nonnull final Type literalValue) {
    super(dataset, idColumn, literalValue);
    this.literalValue = (BooleanType) literalValue;
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
  public static BooleanLiteralPath fromString(@Nonnull final String fhirPath,
      @Nonnull final FhirPath context) {
    check(context.getIdColumn().isPresent());
    final boolean value = fhirPath.equals("true");
    return new BooleanLiteralPath(context.getDataset(), context.getIdColumn().get(),
        new BooleanType(value));
  }

  @Nonnull
  @Override
  public String getExpression() {
    return literalValue.asStringValue();
  }

  @Nonnull
  @Override
  public Boolean getJavaValue() {
    return literalValue.booleanValue();
  }

  @Override
  public Function<Comparable, Column> getComparison(final ComparisonOperation operation) {
    return FhirPath.buildComparison(this, operation.getSparkFunction());
  }

  @Override
  public boolean isComparableTo(@Nonnull final Class<? extends Comparable> type) {
    return BooleanPath.getComparableTypes().contains(type);
  }
}
