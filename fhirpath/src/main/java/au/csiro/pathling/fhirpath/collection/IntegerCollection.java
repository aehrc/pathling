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

import au.csiro.pathling.annotations.UsedByReflection;
import au.csiro.pathling.fhirpath.FhirPathType;
import au.csiro.pathling.fhirpath.Materializable;
import au.csiro.pathling.fhirpath.Numeric;
import au.csiro.pathling.fhirpath.StringCoercible;
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import au.csiro.pathling.fhirpath.comparison.Comparable;
import au.csiro.pathling.fhirpath.definition.NodeDefinition;
import com.google.common.collect.ImmutableSet;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.IntegerType;
import org.hl7.fhir.r4.model.PositiveIntType;
import org.hl7.fhir.r4.model.UnsignedIntType;

/**
 * Represents a FHIRPath expression which refers to an integer typed element.
 *
 * @author John Grimes
 */
public class IntegerCollection extends Collection
    implements Comparable, Numeric, StringCoercible, Materializable {

  private static final Set<FHIRDefinedType> INTEGER_TYPES =
      Set.of(FHIRDefinedType.INTEGER, FHIRDefinedType.UNSIGNEDINT, FHIRDefinedType.POSITIVEINT);

  private static final ImmutableSet<Class<? extends Comparable>> COMPARABLE_TYPES =
      ImmutableSet.of(IntegerCollection.class, DecimalCollection.class);

  /**
   * Creates a new IntegerCollection.
   *
   * @param columnRepresentation the column representation for this collection
   * @param type the FhirPath type
   * @param fhirType the FHIR type
   * @param definition the node definition
   * @param extensionMapColumn the extension map column
   */
  protected IntegerCollection(
      @Nonnull final ColumnRepresentation columnRepresentation,
      @Nonnull final Optional<FhirPathType> type,
      @Nonnull final Optional<FHIRDefinedType> fhirType,
      @Nonnull final Optional<? extends NodeDefinition> definition,
      @Nonnull final Optional<Column> extensionMapColumn) {
    super(columnRepresentation, type, fhirType, definition, extensionMapColumn);
    fhirType.ifPresent(
        t -> {
          if (!INTEGER_TYPES.contains(t)) {
            throw new IllegalArgumentException("FHIR type must be an integer type");
          }
        });
  }

  /**
   * Returns a new instance with the specified column representation and definition.
   *
   * @param columnRepresentation The column representation to use
   * @param definition The definition to use
   * @return A new instance of {@link IntegerCollection}
   */
  @Nonnull
  public static IntegerCollection build(
      @Nonnull final ColumnRepresentation columnRepresentation,
      @Nonnull final Optional<NodeDefinition> definition) {
    return new IntegerCollection(
        columnRepresentation,
        Optional.of(FhirPathType.INTEGER),
        Optional.of(FHIRDefinedType.INTEGER),
        definition,
        Optional.empty());
  }

  /**
   * Returns a new instance with the specified column representation and no definition.
   *
   * @param columnRepresentation The column representation to use
   * @return A new instance of {@link IntegerCollection}
   */
  @Nonnull
  public static IntegerCollection build(final ColumnRepresentation columnRepresentation) {
    return build(columnRepresentation, Optional.empty());
  }

  /**
   * Returns a new instance based upon a literal represented by an {@link IntegerType}.
   *
   * <p>This is required for the reflection-based instantiation of collections used in {@link
   * au.csiro.pathling.projection.ProjectionContext}.
   *
   * @param value The value to use
   * @return A new instance of {@link IntegerCollection}
   */
  @SuppressWarnings("unused")
  @Nonnull
  public static IntegerCollection fromValue(@Nonnull final IntegerType value) {
    return IntegerCollection.fromValue(value.getValue());
  }

  /**
   * Returns a new instance based upon a literal value.
   *
   * @param value The value to use
   * @return A new instance of {@link IntegerCollection}
   */
  @Nonnull
  public static IntegerCollection fromValue(final int value) {
    return IntegerCollection.build(DefaultRepresentation.literal(value));
  }

  /**
   * Returns a new instance based upon a {@link PositiveIntType}.
   *
   * @param value The value to use
   * @return A new instance of {@link IntegerCollection}
   */
  @UsedByReflection
  @Nonnull
  public static IntegerCollection fromValue(@Nonnull final PositiveIntType value) {
    return IntegerCollection.fromValue(value.getValue());
  }

  /**
   * Returns a new instance based upon a {@link UnsignedIntType}.
   *
   * @param value The value to use
   * @return A new instance of {@link IntegerCollection}
   */
  @UsedByReflection
  @Nonnull
  public static IntegerCollection fromValue(@Nonnull final UnsignedIntType value) {
    return IntegerCollection.fromValue(value.getValue());
  }

  /**
   * Returns a new instance, parsed from a FHIRPath literal.
   *
   * @param integerLiteral The FHIRPath representation of the literal
   * @return A new instance of {@link IntegerCollection}
   * @throws NumberFormatException if the literal is malformed
   */
  @Nonnull
  public static IntegerCollection fromLiteral(@Nonnull final String integerLiteral)
      throws NumberFormatException {
    return IntegerCollection.fromValue(Integer.parseInt(integerLiteral));
  }

  /**
   * Returns a set of classes that this collection can be compared to.
   *
   * @return A set of classes that this collection can be compared to
   */
  @Nonnull
  public static ImmutableSet<Class<? extends Comparable>> getComparableTypes() {
    return COMPARABLE_TYPES;
  }

  @Override
  public boolean isComparableTo(@Nonnull final Collection path) {
    return COMPARABLE_TYPES.contains(path.getClass());
  }

  @Nonnull
  @Override
  public Function<Numeric, Collection> getMathOperation(@Nonnull final MathOperation operation) {
    return buildMathOperation(this, operation);
  }

  @Nonnull
  @Override
  public Optional<Column> getNumericValue() {
    return Optional.ofNullable(this.getColumn().elementCast(DataTypes.LongType).getValue());
  }

  /**
   * Builds a math operation result for a collection of Integers.
   *
   * @param source The left operand for the operation
   * @param operation The type of {@link au.csiro.pathling.fhirpath.Numeric.MathOperation}
   * @return A {@link Function} that takes a {@link Numeric} as a parameter, and returns a {@link
   *     Collection}
   */
  @Nonnull
  public static Function<Numeric, Collection> buildMathOperation(
      @Nonnull final Numeric source, @Nonnull final MathOperation operation) {
    return target -> {
      final Column sourceNumeric = checkPresent(source.getNumericValue());
      final Column targetNumeric = checkPresent(target.getNumericValue());
      Column valueColumn = operation.getSparkFunction().apply(sourceNumeric, targetNumeric);

      switch (operation) {
        case ADDITION, SUBTRACTION, MULTIPLICATION, MODULUS:
          if (target instanceof DecimalCollection) {
            valueColumn = valueColumn.cast(DataTypes.LongType);
          }
          return IntegerCollection.build(new DefaultRepresentation(valueColumn));
        case DIVISION:
          final Column numerator =
              source.getColumn().elementCast(DecimalCollection.getDecimalType()).getValue();
          valueColumn = operation.getSparkFunction().apply(numerator, targetNumeric);
          return DecimalCollection.build(new DefaultRepresentation(valueColumn));
        default:
          throw new AssertionError("Unsupported math operation encountered: " + operation);
      }
    };
  }

  @Override
  @Nonnull
  public StringCollection asStringPath() {
    return Collection.defaultAsStringPath(this);
  }

  @Override
  @Nonnull
  public Collection negate() {
    return Numeric.defaultNegate(this);
  }

  @Override
  public boolean convertibleTo(@Nonnull final Collection other) {
    return other
        .getFhirType()
        .filter(t -> t == FHIRDefinedType.DECIMAL || t == FHIRDefinedType.QUANTITY)
        .map(t -> true)
        .orElseGet(() -> super.convertibleTo(other));
  }

  @Override
  public @Nonnull Collection castAs(@Nonnull final Collection other) {
    return other
        .getType()
        .filter(FhirPathType.QUANTITY::equals)
        .map(t -> (Collection) QuantityCollection.fromNumeric(this.getColumn()))
        .orElseGet(() -> super.castAs(other));
  }
}
