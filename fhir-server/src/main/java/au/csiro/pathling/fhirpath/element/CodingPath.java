/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.element;

import static org.apache.spark.sql.functions.*;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.Comparable;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.Materializable;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.literal.CodingLiteral;
import au.csiro.pathling.fhirpath.literal.CodingLiteralPath;
import au.csiro.pathling.fhirpath.literal.NullLiteralPath;
import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Represents a FHIRPath expression which refers to an element of type Coding.
 *
 * @author John Grimes
 */
public class CodingPath extends ElementPath implements Materializable<Coding>, Comparable {

  private static final List<String> EQUALITY_COLUMNS = Arrays
      .asList("system", "code", "version", "display", "userSelected");


  private static final ImmutableSet<Class<? extends Comparable>> COMPARABLE_TYPES = ImmutableSet
      .of(CodingPath.class, CodingLiteralPath.class, NullLiteralPath.class);

  protected CodingPath(@Nonnull final String expression, @Nonnull final Dataset<Row> dataset,
      @Nonnull final Column idColumn, @Nonnull final Optional<Column> eidColumn,
      @Nonnull final Column valueColumn, final boolean singular,
      @Nonnull final Optional<ResourcePath> foreignResource,
      @Nonnull final Optional<Column> thisColumn, @Nonnull final FHIRDefinedType fhirType) {
    super(expression, dataset, idColumn, eidColumn, valueColumn, singular, foreignResource,
        thisColumn, fhirType);
  }

  @Nonnull
  @Override
  public Optional<Coding> getValueFromRow(@Nonnull final Row row, final int columnNumber) {
    return valueFromRow(row, columnNumber);
  }

  /**
   * Gets a value from a row for a Coding or Coding literal.
   *
   * @param row The {@link Row} from which to extract the value
   * @param columnNumber The column number to extract the value from
   * @return A {@link Coding}, or the absence of a value
   */
  @Nonnull
  public static Optional<Coding> valueFromRow(@Nonnull final Row row, final int columnNumber) {
    if (row.isNullAt(columnNumber)) {
      return Optional.empty();
    }

    final Row codingStruct = row.getStruct(columnNumber);

    final String system = codingStruct.getString(codingStruct.fieldIndex("system"));
    final String version = codingStruct.getString(codingStruct.fieldIndex("version"));
    final String code = codingStruct.getString(codingStruct.fieldIndex("code"));
    final String display = codingStruct.getString(codingStruct.fieldIndex("display"));

    final int userSelectedIndex = codingStruct.fieldIndex("userSelected");
    final boolean userSelectedPresent = !codingStruct.isNullAt(userSelectedIndex);

    final Coding coding = new Coding(system, code, display);
    coding.setVersion(version);
    if (userSelectedPresent) {
      coding.setUserSelected(codingStruct.getBoolean(userSelectedIndex));
    }

    return Optional.of(coding);
  }

  /**
   * Builds a comparison function for Coding paths.
   *
   * @param source The path to build the comparison function for
   * @param operation The {@link au.csiro.pathling.fhirpath.Comparable.ComparisonOperation} type to
   * build
   * @return A new {@link Function}
   */
  @Nonnull
  public static Function<Comparable, Column> buildComparison(@Nonnull final Comparable source,
      @Nonnull final ComparisonOperation operation) {
    if (ComparisonOperation.EQUALS.equals(operation)) {
      return Comparable
          .buildComparison(source, codingEqual());
    } else if (ComparisonOperation.NOT_EQUALS.equals(operation)) {
      return Comparable
          .buildComparison(source, codingNotEqual());
    } else {
      throw new InvalidUserInputError(
          "Coding type does not support comparison operator: " + operation);
    }
  }

  @Nonnull
  private static BiFunction<Column, Column, Column> codingEqual() {
    //noinspection OptionalGetWithoutIsPresent
    return (l, r) ->
        functions.when(l.isNull().or(r.isNull()), lit(null))
            .otherwise(
                EQUALITY_COLUMNS.stream()
                    .map(f -> l.getField(f).eqNullSafe(r.getField(f))).reduce(Column::and).get()
            );
  }

  @Nonnull
  private static BiFunction<Column, Column, Column> codingNotEqual() {
    return codingEqual().andThen(functions::not);
  }

  @Nonnull
  public static ImmutableSet<Class<? extends Comparable>> getComparableTypes() {
    return COMPARABLE_TYPES;
  }

  @Override
  @Nonnull
  public Function<Comparable, Column> getComparison(@Nonnull final ComparisonOperation operation) {
    return buildComparison(this, operation);
  }

  @Override
  public boolean isComparableTo(@Nonnull final Class<? extends Comparable> type) {
    return COMPARABLE_TYPES.contains(type);
  }

  @Override
  public boolean canBeCombinedWith(@Nonnull final FhirPath target) {
    return super.canBeCombinedWith(target) || target instanceof CodingLiteralPath;
  }

  @Nonnull
  @Override
  public Column getExtractableColumn() {
    // We test to see if each component contains special characters, so that we know if it needs to
    // be quoted.
    final UnaryOperator<Column> quote = col -> when(col.rlike(CodingLiteral.SPECIAL_CHARACTERS),
        concat(lit("'"), col, lit("'"))).otherwise(col);
    final Column system = quote.apply(getValueColumn().getField("system"));
    final Column code = quote.apply(getValueColumn().getField("code"));
    final Column version = quote.apply(getValueColumn().getField("version"));
    final Column display = quote.apply(getValueColumn().getField("display"));

    // The userSelected component is a Boolean, so we need to cast it to a string.
    final Column userSelected = getValueColumn().getField("userSelected")
        .cast(DataTypes.StringType);
    final Column array = array(system, code, version, display, userSelected);

    // Join the components using the pipe character, replacing nulls with empty strings.
    final Column joinedString = array_join(array, "|", "");
   
    // Trim any trailing pipes off the end.
    return regexp_replace(joinedString, "(\\|+)$", "");
  }

}
