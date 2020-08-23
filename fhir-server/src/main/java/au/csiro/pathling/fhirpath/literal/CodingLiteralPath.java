/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.literal;

import static au.csiro.pathling.utilities.Preconditions.check;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.struct;

import au.csiro.pathling.fhirpath.Comparable;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.element.CodingPath;
import java.util.Arrays;
import java.util.LinkedList;
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
public class CodingLiteralPath extends LiteralPath implements Comparable {

  @Nonnull
  private final Coding literalValue;

  @SuppressWarnings("WeakerAccess")
  protected CodingLiteralPath(@Nonnull final Dataset<Row> dataset, @Nonnull final Column idColumn,
      @Nonnull final Type literalValue) {
    super(dataset, idColumn, literalValue);
    this.literalValue = (Coding) literalValue;
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
  public static CodingLiteralPath fromString(@Nonnull final String fhirPath,
      @Nonnull final FhirPath context) throws IllegalArgumentException {
    check(context.getIdColumn().isPresent());
    final LinkedList<String> codingTokens = new LinkedList<>(Arrays.asList(fhirPath.split("\\|")));
    final Coding coding;
    if (codingTokens.size() == 2) {
      coding = new Coding(codingTokens.get(0), codingTokens.get(1), null);
    } else if (codingTokens.size() == 3) {
      coding = new Coding(codingTokens.get(0), codingTokens.get(2), null);
      coding.setVersion(codingTokens.get(1));
    } else {
      throw new IllegalArgumentException(
          "Coding literal must be of form [system]|[code] or [system]|[version]|[code]");
    }
    return new CodingLiteralPath(context.getDataset(), context.getIdColumn().get(), coding);
  }

  @Nonnull
  @Override
  public String getExpression() {
    return literalValue.getVersion() == null
           ? literalValue.getSystem() + "|" + literalValue.getCode()
           : literalValue.getSystem() + "|" + literalValue.getVersion() + "|" + literalValue
               .getCode();
  }

  @Nonnull
  @Override
  public Coding getJavaValue() {
    return literalValue;
  }

  @Nonnull
  @Override
  public Column getValueColumn() {
    final Coding value = getJavaValue();
    return struct(
        lit(value.getId()).as("id"),
        lit(value.getSystem()).as("system"),
        lit(value.getVersion()).as("version"),
        lit(value.getCode()).as("code"),
        lit(value.getDisplay()).as("display"),
        lit(value.getUserSelected()).as("userSelected"));
  }

  @Override
  public Function<Comparable, Column> getComparison(final ComparisonOperation operation) {
    return CodingPath.buildComparison(this, operation);
  }

  @Override
  public boolean isComparableTo(@Nonnull final Class<? extends Comparable> type) {
    return CodingPath.getComparableTypes().contains(type);
  }
}
