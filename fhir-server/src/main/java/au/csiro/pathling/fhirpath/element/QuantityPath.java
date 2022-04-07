/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.element;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.fhirpath.Comparable;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.literal.NullLiteralPath;
import au.csiro.pathling.fhirpath.literal.QuantityLiteralPath;
import au.csiro.pathling.terminology.ucum.ComparableQuantity;
import com.google.common.collect.ImmutableSet;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Represents a FHIRPath expression which refers to an element of type Quantity.
 *
 * @author John Grimes
 */
public class QuantityPath extends ElementPath implements Comparable {

  public static final ImmutableSet<Class<? extends Comparable>> COMPARABLE_TYPES = ImmutableSet
      .of(QuantityPath.class, QuantityLiteralPath.class, NullLiteralPath.class);

  protected QuantityPath(@Nonnull final String expression, @Nonnull final Dataset<Row> dataset,
      @Nonnull final Column idColumn, @Nonnull final Optional<Column> eidColumn,
      @Nonnull final Column valueColumn, final boolean singular,
      @Nonnull final Optional<ResourcePath> currentResource,
      @Nonnull final Optional<Column> thisColumn, @Nonnull final FHIRDefinedType fhirType) {
    super(expression, dataset, idColumn, eidColumn, valueColumn, singular, currentResource,
        thisColumn, fhirType);
  }

  @Nonnull
  @Override
  public Function<Comparable, Column> getComparison(@Nonnull final ComparisonOperation operation) {
    return buildComparison(this, operation);
  }

  @Nonnull
  public static Function<Comparable, Column> buildComparison(@Nonnull final FhirPath source,
      @Nonnull final ComparisonOperation operation) {
    return target -> {
      final Column comparableSource = callUDF(ComparableQuantity.FUNCTION_NAME,
          source.getValueColumn());
      final Column comparableTarget = callUDF(ComparableQuantity.FUNCTION_NAME,
          target.getValueColumn());
      final Column sourceCode = comparableSource.getField("code");
      final Column targetCode = comparableTarget.getField("code");
      final Column sourceValue = comparableSource.getField("value");
      final Column targetValue = comparableTarget.getField("value");
      final Column compareValues = operation.getSparkFunction().apply(sourceValue, targetValue);
      return when(sourceCode.equalTo(targetCode), compareValues).otherwise(null);
    };
  }

  @Override
  public boolean isComparableTo(@Nonnull final Class<? extends Comparable> type) {
    return COMPARABLE_TYPES.contains(type);
  }

}
