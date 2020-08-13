/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.element;

import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.fhirpath.Comparable;
import au.csiro.pathling.fhirpath.Materializable;
import au.csiro.pathling.fhirpath.literal.CodingLiteralPath;
import au.csiro.pathling.fhirpath.literal.NullLiteralPath;
import com.google.common.collect.ImmutableSet;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Represents a FHIRPath expression which refers to an element of type Coding.
 *
 * @author John Grimes
 */
public class CodingPath extends ElementPath implements Materializable<Coding>, Comparable {

  private static final ImmutableSet<Class<? extends Comparable>> COMPARABLE_TYPES = ImmutableSet
      .of(CodingPath.class, CodingLiteralPath.class, NullLiteralPath.class);

  /**
   * @param expression The FHIRPath representation of this path
   * @param dataset A {@link Dataset} that can be used to evaluate this path against data
   * @param idColumn A {@link Column} within the dataset containing the identity of the subject
   * resource
   * @param valueColumn A {@link Column} within the dataset containing the values of the nodes
   * @param singular An indicator of whether this path represents a single-valued collection
   * @param fhirType The FHIR datatype for this path, note that there can be more than one FHIR
   * type
   */
  public CodingPath(@Nonnull final String expression, @Nonnull final Dataset<Row> dataset,
      @Nonnull final Optional<Column> idColumn, @Nonnull final Column valueColumn,
      final boolean singular, @Nonnull final FHIRDefinedType fhirType) {
    super(expression, dataset, idColumn, valueColumn, singular, fhirType);
  }

  @Nonnull
  @Override
  public Optional<Coding> getValueFromRow(@Nonnull final Row row, final int columnNumber) {
    if (row.isNullAt(columnNumber)) {
      return Optional.empty();
    }

    final Row codingStruct = row.getStruct(columnNumber);

    final String system = codingStruct.getString(row.fieldIndex("system"));
    final String version = codingStruct.getString(row.fieldIndex("version"));
    final String code = codingStruct.getString(row.fieldIndex("code"));
    final String display = codingStruct.getString(row.fieldIndex("display"));
    final boolean userSelected = codingStruct.getBoolean(row.fieldIndex("userSelected"));
    final Coding coding = new Coding(system, code, display);
    coding.setVersion(version);
    coding.setUserSelected(userSelected);

    return Optional.of(coding);
  }

  /**
   * Builds a comparison function for Coding paths.
   *
   * @param source The path to build the comparison function for
   * @param sparkFunction The Spark column function to use
   * @return A new {@link Function}
   */
  @Nonnull
  public static Function<Comparable, Column> buildComparison(@Nonnull final Comparable source,
      @Nonnull final BiFunction<Column, Column, Column> sparkFunction) {
    return (target) -> {
      // If either path is a null literal, we need to bail here as it won't be legal to refer to 
      // fields of a null literal later in the expression.
      if (source instanceof NullLiteralPath || target instanceof NullLiteralPath) {
        return lit(null);
      }

      final Column left = source.getValueColumn();
      final Column right = target.getValueColumn();

      final Column eitherCodingIsIncomplete = left.getField("system").isNull()
          .or(left.getField("code").isNull())
          .or(right.getField("system").isNull())
          .or(right.getField("code").isNull());

      final Column eitherCodingIsMissingVersion = left.getField("version").isNull()
          .or(right.getField("version").isNull());

      final Column versionAgnosticTest = sparkFunction
          .apply(left.getField("system"), right.getField("system"))
          .and(sparkFunction.apply(left.getField("code"), right.getField("code")));

      final Column fullEqualityTest = versionAgnosticTest
          .and(sparkFunction.apply(left.getField("version"), right.getField("version")));

      return when(eitherCodingIsIncomplete, null)
          .when(eitherCodingIsMissingVersion, versionAgnosticTest)
          .otherwise(fullEqualityTest);
    };
  }

  @Nonnull
  public static ImmutableSet<Class<? extends Comparable>> getComparableTypes() {
    return COMPARABLE_TYPES;
  }

  @Override
  public Function<Comparable, Column> getComparison(
      final BiFunction<Column, Column, Column> sparkFunction) {
    return buildComparison(this, sparkFunction);
  }

  @Override
  public boolean isComparableTo(@Nonnull final Class<? extends Comparable> type) {
    return COMPARABLE_TYPES.contains(type);
  }

}
