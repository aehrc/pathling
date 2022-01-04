/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.element;

import au.csiro.pathling.fhirpath.Comparable;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.Materializable;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.literal.NullLiteralPath;
import au.csiro.pathling.fhirpath.literal.StringLiteralPath;
import com.google.common.collect.ImmutableSet;
import java.util.Optional;
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

  protected StringPath(@Nonnull final String expression, @Nonnull final Dataset<Row> dataset,
      @Nonnull final Column idColumn, @Nonnull final Optional<Column> eidColumn,
      @Nonnull final Column valueColumn, final boolean singular,
      @Nonnull final Optional<ResourcePath> foreignResource,
      @Nonnull final Optional<Column> thisColumn, @Nonnull final FHIRDefinedType fhirType) {
    super(expression, dataset, idColumn, eidColumn, valueColumn, singular, foreignResource,
        thisColumn, fhirType);
  }

  @Nonnull
  @Override
  public Optional<PrimitiveType> getValueFromRow(@Nonnull final Row row,
      final int columnNumber) {
    return valueFromRow(row, columnNumber, getFhirType());
  }

  /**
   * Gets a value from a row for a String or String literal.
   *
   * @param row The {@link Row} from which to extract the value
   * @param columnNumber The column number to extract the value from
   * @param fhirType The FHIR type to assume when extracting the value
   * @return A {@link PrimitiveType}, or the absence of a value
   */
  @Nonnull
  public static Optional<PrimitiveType> valueFromRow(@Nonnull final Row row, final int columnNumber,
      final FHIRDefinedType fhirType) {
    if (row.isNullAt(columnNumber)) {
      return Optional.empty();
    }
    switch (fhirType) {
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
  @Nonnull
  public Function<Comparable, Column> getComparison(@Nonnull final ComparisonOperation operation) {
    return Comparable.buildComparison(this, operation.getSparkFunction());
  }

  @Override
  public boolean isComparableTo(@Nonnull final Class<? extends Comparable> type) {
    return COMPARABLE_TYPES.contains(type);
  }

  @Override
  public boolean canBeCombinedWith(@Nonnull final FhirPath target) {
    return super.canBeCombinedWith(target) || target instanceof StringLiteralPath;
  }

}
