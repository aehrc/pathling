/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.literal;

import static org.apache.spark.sql.functions.lit;

import au.csiro.pathling.fhirpath.Comparable;
import au.csiro.pathling.fhirpath.FhirPath;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.StringType;

/**
 * Represents the null literal ({@code {}}, an empty collection) in FHIRPath.
 *
 * @author John Grimes
 */
public class NullLiteralPath extends LiteralPath implements Comparable {

  private static final String EXPRESSION = "{}";

  @SuppressWarnings("WeakerAccess")
  protected NullLiteralPath(@Nonnull final Dataset<Row> dataset, @Nonnull final Column idColumn) {
    // We put a dummy String value in here as a placeholder so that we can satisfy the nullability 
    // constraints within LiteralValue. It is never accessed.
    super(dataset, idColumn, new StringType(EXPRESSION));
    // But then we also need to override the value column
    // to be a null literal rather then the placeholder value literal.
    this.valueColumn = lit(null);
  }

  /**
   * Get a new instance of this class.
   *
   * @param context The input context to use to build a {@link Dataset} to represent this element
   * @return A shiny new NullLiteralPath
   */
  @Nonnull
  public static NullLiteralPath build(@Nonnull final FhirPath context) {
    return new NullLiteralPath(context.getDataset(), context.getIdColumn());
  }

  @Nonnull
  @Override
  public String getExpression() {
    return EXPRESSION;
  }

  @Nullable
  @Override
  public Object getJavaValue() {
    return null;
  }

  @Override
  @Nonnull
  public Function<Comparable, Column> getComparison(@Nonnull final ComparisonOperation operation) {
    // Comparing an empty collection with anything always results in an empty collection.
    return (target) -> lit(null);
  }

  @Override
  public boolean isComparableTo(@Nonnull final Class<? extends Comparable> type) {
    return true;
  }

  @Override
  public boolean canBeCombinedWith(@Nonnull final FhirPath target) {
    // A null literal can be combined with any other path.
    return true;
  }

}
