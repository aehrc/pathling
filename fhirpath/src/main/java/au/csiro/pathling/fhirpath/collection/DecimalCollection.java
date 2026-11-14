/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.csiro.pathling.fhirpath.collection;

import static au.csiro.pathling.utilities.Preconditions.checkPresent;

import au.csiro.pathling.definition.NodeDefinition;
import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.FhirPathType;
import au.csiro.pathling.fhirpath.Materializable;
import au.csiro.pathling.fhirpath.Numeric;
import au.csiro.pathling.fhirpath.StringCoercible;
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import au.csiro.pathling.fhirpath.column.StoredDecimal;
import au.csiro.pathling.fhirpath.comparison.ColumnComparator;
import au.csiro.pathling.fhirpath.comparison.Comparable;
import au.csiro.pathling.fhirpath.comparison.DecimalComparator;
import au.csiro.pathling.fhirpath.comparison.StoredDecimalComparator;
import au.csiro.pathling.sql.misc.DecimalToLiteral;
import au.csiro.pathling.utilities.SystemDecimal;
import jakarta.annotation.Nonnull;
import java.math.BigDecimal;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import org.hl7.fhir.r4.model.DecimalType;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Represents a collection of decimals.
 *
 * <p>A decimal is either stored or computed, as its collection's definition tells (decision 147). A
 * stored decimal, reached by traversal, is held as its stored text and its numeric annotation (see
 * {@link StoredDecimal}), and keeps its definition through {@code where()}, {@code first()}, {@code
 * distinct()} and a union of stored decimals. A computed decimal, such as a literal, the result of
 * arithmetic or {@code toDecimal()}, or an integer promoted to a decimal, has no definition, and is
 * held as the engine's System.Decimal, {@link SystemDecimal#TYPE}. A stored decimal is converted to
 * a computed one wherever it is computed with ({@link #toEngineForm()}), and is compared and
 * deduplicated by its number either way.
 *
 * @author John Grimes
 */
public class DecimalCollection extends Collection
    implements Comparable, Numeric, StringCoercible, Materializable {

  /**
   * Creates a new DecimalCollection.
   *
   * @param columnRepresentation the column representation for this collection
   * @param fhirPathType the FhirPath type
   * @param fhirType the FHIR type
   * @param definition the node definition
   */
  protected DecimalCollection(
      @Nonnull final ColumnRepresentation columnRepresentation,
      @Nonnull final Optional<FhirPathType> fhirPathType,
      @Nonnull final Optional<FHIRDefinedType> fhirType,
      @Nonnull final Optional<NodeDefinition> definition) {
    super(columnRepresentation, fhirPathType, fhirType, definition);
  }

  /**
   * Returns a new instance with the specified column and definition.
   *
   * @param columnRepresentation The column to use
   * @param definition The definition to use
   * @return A new instance of {@link DecimalCollection}
   */
  @Nonnull
  public static DecimalCollection build(
      @Nonnull final ColumnRepresentation columnRepresentation,
      @Nonnull final Optional<NodeDefinition> definition) {
    return new DecimalCollection(
        columnRepresentation,
        Optional.of(FhirPathType.DECIMAL),
        Optional.of(FHIRDefinedType.DECIMAL),
        definition);
  }

  /**
   * Returns a new instance with the specified column and unknown definition.
   *
   * @param columnRepresentation The column to use
   * @return A new instance of {@link DecimalCollection}
   */
  @Nonnull
  public static DecimalCollection build(@Nonnull final ColumnRepresentation columnRepresentation) {
    return DecimalCollection.build(columnRepresentation, Optional.empty());
  }

  /**
   * Returns a new instance, parsed from a FHIRPath literal.
   *
   * @param literal the FHIRPath representation of the literal
   * @return a new instance of {@link DecimalCollection}
   * @throws NumberFormatException if the literal is malformed
   */
  @Nonnull
  public static DecimalCollection fromLiteral(@Nonnull final String literal)
      throws NumberFormatException {
    final BigDecimal value = parseLiteral(literal);
    return DecimalCollection.build(DefaultRepresentation.literal(value));
  }

  /**
   * Returns a new instance based upon a {@link DecimalType}.
   *
   * @param value The value to use
   * @return A new instance of {@link DecimalCollection}
   */
  @Nonnull
  public static DecimalCollection fromValue(@Nonnull final DecimalType value) {
    return DecimalCollection.build(DefaultRepresentation.literal(value.getValue()));
  }

  /**
   * Parses a FHIRPath decimal literal into a {@link BigDecimal}.
   *
   * @param literal The FHIRPath representation of the literal
   * @return The parsed {@link BigDecimal}
   */
  @Nonnull
  public static BigDecimal parseLiteral(final @Nonnull String literal) {
    final BigDecimal value = new BigDecimal(literal);

    // A literal with more integer digits than the type holds would become null wherever it is cast
    // to the type. Together with the scale, this also bounds the literal's precision.
    if (value.precision() - value.scale() > SystemDecimal.INTEGER_DIGITS) {
      throw new InvalidUserInputError(
          "Decimal literal exceeded maximum integer digits supported ("
              + SystemDecimal.INTEGER_DIGITS
              + "): "
              + literal);
    }
    if (value.scale() > DecimalCollection.getDecimalType().scale()) {
      throw new InvalidUserInputError(
          "Decimal literal exceeded maximum scale supported ("
              + DecimalCollection.getDecimalType().scale()
              + "): "
              + literal);
    }
    return value;
  }

  /**
   * Gets the decimal data type used for representing decimal values in Spark, which is the type of
   * the engine's System.Decimal, {@link SystemDecimal#TYPE}.
   *
   * @return the {@link org.apache.spark.sql.types.DataType} used for representing decimal values in
   *     Spark
   */
  @Nonnull
  public static org.apache.spark.sql.types.DecimalType getDecimalType() {
    return SystemDecimal.TYPE;
  }

  /**
   * Returns this collection with its values as {@link SystemDecimal#TYPE}, so that decimals of
   * different precisions share one SQL type and can be held in one array.
   *
   * <p>The cast is a safe one, so an out-of-range value yields null rather than raising under ANSI
   * mode, independent of the session-wide setting. It keeps the cardinality of the collection.
   *
   * @return this collection, with its values as {@link SystemDecimal#TYPE}
   */
  @Override
  @Nonnull
  public DecimalCollection withSharedSqlType() {
    // Stored decimals combined into one array are reconciled by the merge, as their annotation
    // fields can differ.
    return isStored() ? this : copyWith(getColumn().elementTryCast(getDecimalType()));
  }

  /**
   * Returns whether the values of this collection are stored decimals, as traversal reached them
   * from the data, rather than computed ones. A computed decimal has no definition.
   *
   * @return true where the values are stored decimals
   */
  public boolean isStored() {
    return getDefinition().isPresent();
  }

  /**
   * Stored decimals are held in the shape the stored data gives them, so a combination of them
   * alone keeps them stored (decision 147). A combination with a computed decimal converts them.
   *
   * @return true where the values are stored decimals
   */
  @Override
  public boolean holdsStoredStructures() {
    return isStored();
  }

  /**
   * Gets this collection as computed decimals: a stored decimal is taken as its number, the stored
   * numeric annotation or else its decoded text, deferred until it is read (decision 142, D8).
   *
   * @return the collection, as computed decimals
   */
  @Override
  @Nonnull
  public DecimalCollection toEngineForm() {
    if (!isStored()) {
      return this;
    }
    // A decimal held with its id and extensions that has no value has no number, and is left out.
    // A null item, as distinct from a structure without a value, is kept as a null item: a decimal
    // whose number does not decode (ticket 50), and equally an element absent at a position of a
    // repeating parent, wherever one reaches the conversion.
    final ColumnRepresentation decimals =
        holdsPrimitiveMetadata()
            ? getColumn()
                .vectorize(
                    array ->
                        functions.filter(
                            array,
                            decimal ->
                                decimal.isNull().or(StoredDecimal.text(decimal).isNotNull())),
                    UnaryOperator.identity())
            : getColumn();
    return DecimalCollection.build(decimals.transform(StoredDecimal::number));
  }

  @Override
  public boolean isComparableTo(@Nonnull final Collection path) {
    return IntegerCollection.getComparableTypes().contains(path.getClass());
  }

  @Nonnull
  @Override
  public Function<Numeric, Collection> getMathOperation(@Nonnull final MathOperation operation) {
    return target -> {
      final Column sourceNumeric = checkPresent(this.getNumericValue());
      final Column targetNumeric = checkPresent(target.getNumericValue());
      Column result = operation.getSparkFunction().apply(sourceNumeric, targetNumeric);

      return switch (operation) {
        case ADDITION, SUBTRACTION, MULTIPLICATION, DIVISION, MODULUS -> {
          // Safe cast so a result that overflows System.Decimal yields NULL rather than raising
          // under ANSI mode, independent of the session-wide setting.
          result = result.try_cast(getDecimalType());
          yield DecimalCollection.build(new DefaultRepresentation(result));
        }
      };
    };
  }

  @Nonnull
  @Override
  public Optional<Column> getNumericValue() {
    return Optional.of(toEngineForm().getColumn().getValue());
  }

  /**
   * {@inheritDoc}
   *
   * <p>A stored decimal is given as its stored text (decision 147), and a computed one as its
   * number, without trailing fractional zeros.
   */
  @Override
  @Nonnull
  public StringCollection asStringPath() {
    if (isStored()) {
      return asSingular().map(cr -> cr.transform(StoredDecimal::text), StringCollection::build);
    }
    // Convert the decimal to a singular string using a UDF to ensure correct formatting.
    return asSingular()
        .map(
            cr -> cr.callUdf(DecimalToLiteral.FUNCTION_NAME, DefaultRepresentation.empty()),
            StringCollection::build);
  }

  @Nonnull
  @Override
  public DecimalCollection copyWith(@Nonnull final ColumnRepresentation newValue) {
    return (DecimalCollection) super.copyWith(newValue);
  }

  @Override
  @Nonnull
  public Collection negate() {
    return Numeric.defaultNegate(toEngineForm());
  }

  /**
   * {@inheritDoc}
   *
   * <p>A decimal is output as its number, a stored one as well as a computed one, until a stored
   * decimal's text reaches views (ticket 16).
   */
  @Override
  @Nonnull
  public Column toExternalValue() {
    return toEngineForm()
        .getColumn()
        .transformWithUdf(DecimalToLiteral.FUNCTION_NAME, DefaultRepresentation.empty())
        .getValue();
  }

  /**
   * {@inheritDoc}
   *
   * <p>A decimal is given to a caller as its number, the engine's System.Decimal, whether stored or
   * computed (decision 147).
   */
  @Override
  @Nonnull
  public ColumnRepresentation getOutputColumn() {
    return toEngineForm().getColumn();
  }

  @Override
  public boolean convertibleTo(@Nonnull final Collection other) {
    return other
        .getType()
        .filter(FhirPathType.QUANTITY::equals)
        .map(t -> true)
        .orElseGet(() -> super.convertibleTo(other));
  }

  @Override
  @Nonnull
  public Collection castAs(@Nonnull final Collection other) {
    return other
        .getType()
        .filter(FhirPathType.QUANTITY::equals)
        .map(t -> (Collection) QuantityCollection.fromNumeric(toEngineForm().getColumn()))
        .orElseGet(() -> super.castAs(other));
  }

  @Override
  @Nonnull
  public ColumnComparator getComparator() {
    if (!isStored()) {
      return DecimalComparator.getInstance();
    }
    return holdsPrimitiveMetadata()
        ? StoredDecimalComparator.withMetadata()
        : StoredDecimalComparator.getInstance();
  }
}
