/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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

import au.csiro.pathling.encoders.datatypes.DecimalCustomCoder;
import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.FhirPathType;
import au.csiro.pathling.fhirpath.Materializable;
import au.csiro.pathling.fhirpath.Numeric;
import au.csiro.pathling.fhirpath.StringCoercible;
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import au.csiro.pathling.fhirpath.comparison.ColumnComparator;
import au.csiro.pathling.fhirpath.comparison.Comparable;
import au.csiro.pathling.fhirpath.comparison.DecimalComparator;
import au.csiro.pathling.fhirpath.definition.NodeDefinition;
import au.csiro.pathling.sql.misc.DecimalToLiteral;
import jakarta.annotation.Nonnull;
import java.math.BigDecimal;
import java.util.Optional;
import java.util.function.Function;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.DecimalType;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Represents a FHIRPath decimal literal.
 *
 * @author John Grimes
 */
public class DecimalCollection extends Collection
    implements Comparable, Numeric, StringCoercible, Materializable {

  /** The Spark SQL decimal type used for FHIR decimal values. */
  public static final org.apache.spark.sql.types.DecimalType DECIMAL_TYPE =
      DataTypes.createDecimalType(DecimalCustomCoder.precision(), DecimalCustomCoder.scale());

  /**
   * Creates a new DecimalCollection.
   *
   * @param columnRepresentation the column representation for this collection
   * @param fhirPathType the FhirPath type
   * @param fhirType the FHIR type
   * @param definition the node definition
   * @param extensionMapColumn the extension map column
   */
  protected DecimalCollection(
      @Nonnull final ColumnRepresentation columnRepresentation,
      @Nonnull final Optional<FhirPathType> fhirPathType,
      @Nonnull final Optional<FHIRDefinedType> fhirType,
      @Nonnull final Optional<NodeDefinition> definition,
      @Nonnull final Optional<Column> extensionMapColumn) {
    super(columnRepresentation, fhirPathType, fhirType, definition, extensionMapColumn);
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
        definition,
        Optional.empty());
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

    if (value.precision() > DecimalCollection.getDecimalType().precision()) {
      throw new InvalidUserInputError(
          "Decimal literal exceeded maximum precision supported ("
              + DecimalCollection.getDecimalType().precision()
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
   * @return the {@link org.apache.spark.sql.types.DataType} used for representing decimal values in
   *     Spark
   */
  public static org.apache.spark.sql.types.DecimalType getDecimalType() {
    return DECIMAL_TYPE;
  }

  /**
   * Normalizes this decimal collection to use the standard DECIMAL(32,6) type. This ensures type
   * compatibility when combining decimal values with different precisions.
   *
   * @return a new DecimalCollection with normalized decimal type
   */
  @Nonnull
  public DecimalCollection normalizeDecimalType() {
    final Column normalizedArray =
        getColumn().plural().getValue().cast(DataTypes.createArrayType(DECIMAL_TYPE));
    return (DecimalCollection) copyWithColumn(normalizedArray);
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
          result = result.cast(getDecimalType());
          yield DecimalCollection.build(new DefaultRepresentation(result));
        }
      };
    };
  }

  @Nonnull
  @Override
  public Optional<Column> getNumericValue() {
    return Optional.of(this.getColumn().getValue());
  }

  @Override
  @Nonnull
  public StringCollection asStringPath() {
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
    return Numeric.defaultNegate(this);
  }

  @Override
  @Nonnull
  public Column toExternalValue() {
    return getColumn()
        .transformWithUdf(DecimalToLiteral.FUNCTION_NAME, DefaultRepresentation.empty())
        .getValue();
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
        .map(t -> (Collection) QuantityCollection.fromNumeric(this.getColumn()))
        .orElseGet(() -> super.castAs(other));
  }

  @Override
  @Nonnull
  public ColumnComparator getComparator() {
    return DecimalComparator.getInstance();
  }
}
