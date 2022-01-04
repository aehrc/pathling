/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.literal;

import static au.csiro.pathling.utilities.Preconditions.check;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.struct;

import au.csiro.pathling.fhirpath.Comparable;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.Materializable;
import au.csiro.pathling.fhirpath.element.CodingPath;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.Nonnull;
import lombok.Getter;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Type;

/**
 * Represents a FHIRPath Coding literal.
 *
 * @author John Grimes
 */
@Getter
public class CodingLiteralPath extends LiteralPath implements Materializable<Coding>, Comparable {

  @SuppressWarnings("WeakerAccess")
  protected CodingLiteralPath(@Nonnull final Dataset<Row> dataset, @Nonnull final Column idColumn,
      @Nonnull final Type literalValue) {
    super(dataset, idColumn, literalValue);
    check(literalValue instanceof Coding);
  }

  /**
   * Returns a new instance, parsed from a FHIRPath literal.
   *
   * @param fhirPath The FHIRPath representation of the literal
   * @param context An input context that can be used to build a {@link Dataset} to represent the
   * literal
   * @return A new instance of {@link LiteralPath}
   * @throws IllegalArgumentException if the literal is malformed
   */
  @Nonnull
  public static CodingLiteralPath fromString(@Nonnull final CharSequence fhirPath,
      @Nonnull final FhirPath context) throws IllegalArgumentException {
    return new CodingLiteralPath(context.getDataset(), context.getIdColumn(),
        CodingLiteral.fromString(fhirPath));
  }

  @Nonnull
  @Override
  public String getExpression() {
    return CodingLiteral.toLiteral(getLiteralValue());

  }

  @Override
  public Coding getLiteralValue() {
    return (Coding) literalValue;
  }

  @Nonnull
  @Override
  public Coding getJavaValue() {
    return getLiteralValue();
  }

  @Nonnull
  @Override
  public Column buildValueColumn() {
    final Coding value = getJavaValue();
    return struct(
        lit(value.getId()).as("id"),
        lit(value.getSystem()).as("system"),
        lit(value.getVersion()).as("version"),
        lit(value.getCode()).as("code"),
        lit(value.getDisplay()).as("display"),
        lit(value.hasUserSelected() ? value.getUserSelected() : null).as("userSelected"));
  }

  @Override
  @Nonnull
  public Function<Comparable, Column> getComparison(@Nonnull final ComparisonOperation operation) {
    return CodingPath.buildComparison(this, operation);
  }

  @Override
  public boolean isComparableTo(@Nonnull final Class<? extends Comparable> type) {
    return CodingPath.getComparableTypes().contains(type);
  }

  @Nonnull
  @Override
  public Optional<Coding> getValueFromRow(@Nonnull final Row row, final int columnNumber) {
    return CodingPath.valueFromRow(row, columnNumber);
  }

  @Override
  public boolean canBeCombinedWith(@Nonnull final FhirPath target) {
    return super.canBeCombinedWith(target) || target instanceof CodingPath;
  }

  @Nonnull
  @Override
  public Column getExtractableColumn() {
    return lit(CodingLiteral.toLiteral(getLiteralValue()));
  }

}
