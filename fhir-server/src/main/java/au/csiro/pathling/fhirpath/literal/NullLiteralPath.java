/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.literal;

import static au.csiro.pathling.utilities.Preconditions.check;
import static org.apache.spark.sql.functions.lit;

import au.csiro.pathling.fhirpath.Comparable;
import au.csiro.pathling.fhirpath.FhirPath;
import java.util.Optional;
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

  private NullLiteralPath(@Nonnull final Dataset<Row> dataset, @Nonnull final Column idColumn) {
    // We put a dummy String value in here as a placeholder so that we can satisfy the nullability 
    // constraints within LiteralValue. It is never accessed.
    super(dataset, idColumn, new StringType(EXPRESSION));
    this.dataset = dataset;
    this.idColumn = idColumn;
  }

  /**
   * Get a new instance of this class.
   *
   * @param context The input context to use to build a {@link Dataset} to represent this element
   * @return A shiny new NullLiteralPath
   */
  @Nonnull
  public static NullLiteralPath build(@Nonnull final FhirPath context) {
    check(context.getIdColumn().isPresent());
    return new NullLiteralPath(context.getDataset(), context.getIdColumn().get());
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
  public Function<Comparable, Column> getComparison(final ComparisonOperation operation) {
    // Comparing an empty collection with anything always results in an empty collection.
    return (target) -> lit(null);
  }

  @Override
  public boolean isComparableTo(@Nonnull final Class<? extends Comparable> type) {
    return true;
  }

  @Nonnull
  @Override
  public NullLiteralPath copy(@Nonnull final String expression,
      @Nonnull final Dataset<Row> dataset, @Nonnull final Optional<Column> idColumn,
      @Nonnull final Column valueColumn, final boolean singular) {
    check(idColumn.isPresent());
    return new NullLiteralPath(dataset, idColumn.get()) {
      @Nonnull
      @Override
      public String getExpression() {
        return expression;
      }
    };
  }

}
