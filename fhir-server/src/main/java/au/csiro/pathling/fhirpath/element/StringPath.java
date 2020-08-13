/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.element;

import au.csiro.pathling.fhirpath.Comparable;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.Materializable;
import au.csiro.pathling.fhirpath.literal.NullLiteralPath;
import au.csiro.pathling.fhirpath.literal.StringLiteralPath;
import com.google.common.collect.ImmutableSet;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.*;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Represents a FHIRPath expression which refers to a string typed element.
 *
 * @author John Grimes
 */
public class StringPath extends ElementPath implements Materializable<PrimitiveType>, Comparable {

  private static final ImmutableSet<Class<? extends Comparable>> COMPARABLE_TYPES = ImmutableSet
      .of(StringPath.class, StringLiteralPath.class, NullLiteralPath.class);

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
  public StringPath(@Nonnull final String expression, @Nonnull final Dataset<Row> dataset,
      @Nonnull final Optional<Column> idColumn, @Nonnull final Column valueColumn,
      final boolean singular,
      @Nonnull final FHIRDefinedType fhirType) {
    super(expression, dataset, idColumn, valueColumn, singular, fhirType);
  }

  @Nonnull
  @Override
  public Optional<PrimitiveType> getValueFromRow(@Nonnull final Row row,
      final int columnNumber) {
    if (row.isNullAt(columnNumber)) {
      return Optional.empty();
    }
    switch (getFhirType()) {
      case URI:
        return Optional.of(new UriType(row.getString(columnNumber)));
      case CODE:
        return Optional.of(new CodeType(row.getString(columnNumber)));
      case OID:
        return Optional.of(new OidType(row.getString(columnNumber)));
      case ID:
        return Optional.of(new IdType(row.getString(columnNumber)));
      case UUID:
        return Optional.of(new UuidType(row.getString(columnNumber)));
      case MARKDOWN:
        return Optional.of(new MarkdownType(row.getString(columnNumber)));
      case BASE64BINARY:
        return Optional.of(new Base64BinaryType(row.getString(columnNumber)));
      default:
        return Optional.of(new StringType(row.getString(columnNumber)));
    }
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
