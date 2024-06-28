/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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
import au.csiro.pathling.fhirpath.column.DecimalRepresentation;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import au.csiro.pathling.fhirpath.definition.NodeDefinition;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.LongType;
import org.hl7.fhir.r4.model.DecimalType;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Represents a FHIRPath decimal literal.
 *
 * @author John Grimes
 */
public class DecimalCollection extends Collection implements Materializable<DecimalType>,
    StringCoercible {

  private static final org.apache.spark.sql.types.DecimalType DECIMAL_TYPE = DataTypes
      .createDecimalType(DecimalCustomCoder.precision(), DecimalCustomCoder.scale());

  protected DecimalCollection(@Nonnull final ColumnRepresentation columnRepresentation,
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
  public static DecimalCollection build(@Nonnull final ColumnRepresentation columnRepresentation,
      @Nonnull final Optional<NodeDefinition> definition) {
    return new DecimalCollection(columnRepresentation, Optional.of(FhirPathType.DECIMAL),
        Optional.of(FHIRDefinedType.DECIMAL), definition);
  }

  /**
   * Returns a new instance with the specified column and unknown definition.
   *
   * @param columnRepresentation The column to use
   * @return A new instance of {@link DecimalCollection}
   */
  @Nonnull
  public static DecimalCollection build(@Nonnull final DecimalRepresentation columnRepresentation) {
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
    return DecimalCollection.build(
        (DecimalRepresentation) DefaultRepresentation.literal(value));
  }

  /**
   * Returns a new instance based upon a {@link DecimalType}.
   *
   * @param value The value to use
   * @return A new instance of {@link DecimalCollection}
   */
  @Nonnull
  public static DecimalCollection fromValue(@Nonnull final DecimalType value) {
    return DecimalCollection.build(
        (DecimalRepresentation) DefaultRepresentation.literal(value.getValue()));
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
              + DecimalCollection.getDecimalType()
              .precision() + "): " + literal);
    }
    if (value.scale() > DecimalCollection.getDecimalType().scale()) {
      throw new InvalidUserInputError(
          "Decimal literal exceeded maximum scale supported (" + DecimalCollection.getDecimalType()
              .scale() + "): " + literal);
    }
    return value;
  }

  /**
   * @return the {@link org.apache.spark.sql.types.DataType} used for representing decimal values in
   * Spark
   */
  public static org.apache.spark.sql.types.DecimalType getDecimalType() {
    return DECIMAL_TYPE;
  }

  @Override
  public boolean isComparableTo(@Nonnull final Collection path) {
    return IntegerCollection.getComparableTypes().contains(path.getClass());
  }

  @Nonnull
  @Override
  public Function<Numeric, Collection> getMathOperation(@Nonnull final MathOperation operation) {
    return target -> {
      final Column sourceNumeric = checkPresent(((Numeric) this).getNumericValue());
      final Column targetNumeric = checkPresent(target.getNumericValue());
      Column result = operation.getSparkFunction().apply(sourceNumeric, targetNumeric);

      switch (operation) {
        case ADDITION:
        case SUBTRACTION:
        case MULTIPLICATION:
        case DIVISION:
          result = result.cast(getDecimalType());
          return DecimalCollection.build(new DecimalRepresentation(result));
        case MODULUS:
          result = result.cast(DataTypes.LongType);
          return IntegerCollection.build(new DefaultRepresentation(result));
        default:
          throw new AssertionError("Unsupported math operation encountered: " + operation);
      }
    };
  }

  @Nonnull
  @Override
  public Optional<Column> getNumericValue() {
    return Optional.of(this.getColumn().getValue());
  }

  @Nonnull
  @Override
  public Optional<Column> getNumericContext() {
    return this.getNumericValue();
  }

  @Nonnull
  @Override
  public Optional<DecimalType> getFhirValueFromRow(@Nonnull final Row row, final int columnNumber) {
    if (row.isNullAt(columnNumber)) {
      return Optional.empty();
    }
    // We support the extraction of Decimal values from columns with the long type. This will be
    // used in the future to support things like casting large numbers to Decimal to work around the
    // maximum Integer limit.
    if (row.schema().fields()[columnNumber].dataType() instanceof LongType) {
      final long longValue = row.getLong(columnNumber);
      return Optional.of(new DecimalType(longValue));
    } else {
      final BigDecimal decimal = row.getDecimal(columnNumber);

      if (decimal.precision() > getDecimalType().precision()) {
        throw new InvalidUserInputError(
            "Attempt to return a Decimal value with greater than supported precision");
      }
      if (decimal.scale() > getDecimalType().scale()) {
        return Optional.of(
            new DecimalType(decimal.setScale(getDecimalType().scale(), RoundingMode.HALF_UP)));
      }

      return Optional.of(new DecimalType(decimal));
    }
  }

  @Override
  @Nonnull
  public StringCollection asStringPath() {
    return map(ColumnRepresentation::asString, StringCollection::build);
  }

  @Nonnull
  @Override
  public DecimalCollection copyWith(@Nonnull final ColumnRepresentation newValue) {
    return (DecimalCollection) super.copyWith(newValue);
  }

}
