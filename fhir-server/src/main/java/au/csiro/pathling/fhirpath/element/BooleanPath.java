/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.element;

import au.csiro.pathling.fhirpath.Comparable;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.Materializable;
import au.csiro.pathling.fhirpath.literal.BooleanLiteralPath;
import au.csiro.pathling.fhirpath.literal.NullLiteralPath;
import com.google.common.collect.ImmutableSet;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Represents a FHIRPath expression which refers to a boolean typed element.
 *
 * @author John Grimes
 */
public class BooleanPath extends ElementPath implements Materializable<BooleanType>, Comparable {

  private static final ImmutableSet<Class<? extends Comparable>> COMPARABLE_TYPES = ImmutableSet
      .of(BooleanPath.class, BooleanLiteralPath.class, NullLiteralPath.class);

  /**
   * @param expression The FHIRPath representation of this path
   * @param dataset A {@link Dataset} that can be used to evaluate this path against data
   * @param idColumn A {@link Column} within the dataset containing the identity of the subject
   * resource
   * @param valueColumn A {@link Column} within the dataset containing the values of the nodes
   * @param singular An indicator of whether this path represents a single-valued collection
   * @param fhirType The FHIR datatype for this path, note that there can be more than one FHIR type
   * to each FHIRPath type
   */
  public BooleanPath(@Nonnull final String expression, @Nonnull final Dataset<Row> dataset,
      @Nonnull final Column idColumn, @Nonnull final Column valueColumn, final boolean singular,
      @Nonnull final FHIRDefinedType fhirType) {
    super(expression, dataset, idColumn, valueColumn, singular, fhirType);
  }

  @Nonnull
  @Override
  public Optional<BooleanType> getValueFromRow(@Nonnull final Row row, final int columnNumber) {
    if (row.isNullAt(columnNumber)) {
      return Optional.empty();
    }
    return Optional.of(new BooleanType(row.getBoolean(columnNumber)));
  }

  @Nonnull
  public static ImmutableSet<Class<? extends Comparable>> getComparableTypes() {
    return COMPARABLE_TYPES;
  }

  @Override
  public Function<Comparable, Column> getComparison(
      final BiFunction<Column, Column, Column> sparkFunction) {
    return FhirPath.buildComparison(this, sparkFunction);
  }

  @Override
  public boolean isComparableTo(@Nonnull final Class<? extends Comparable> type) {
    return COMPARABLE_TYPES.contains(type);
  }

}
