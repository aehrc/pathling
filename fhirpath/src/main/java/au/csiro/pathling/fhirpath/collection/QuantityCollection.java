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

import au.csiro.pathling.errors.UnsupportedFhirPathFeatureError;
import au.csiro.pathling.fhirpath.FhirPathQuantity;
import au.csiro.pathling.fhirpath.FhirPathType;
import au.csiro.pathling.fhirpath.Numeric;
import au.csiro.pathling.fhirpath.StringCoercible;
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import au.csiro.pathling.fhirpath.column.QuantityValue;
import au.csiro.pathling.fhirpath.comparison.ColumnComparator;
import au.csiro.pathling.fhirpath.comparison.Comparable;
import au.csiro.pathling.fhirpath.comparison.QuantityComparator;
import au.csiro.pathling.fhirpath.definition.ElementDefinition;
import au.csiro.pathling.fhirpath.definition.NodeDefinition;
import au.csiro.pathling.fhirpath.definition.defaults.DefaultCompositeDefinition;
import au.csiro.pathling.fhirpath.definition.defaults.DefaultPrimitiveDefinition;
import au.csiro.pathling.fhirpath.encoding.QuantityEncoding;
import au.csiro.pathling.sql.misc.QuantityToLiteral;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import org.apache.spark.sql.Column;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Represents a FHIRPath expression which refers to an element of type Quantity.
 *
 * @author John Grimes
 */
public class QuantityCollection extends Collection implements Comparable, StringCoercible, Numeric {

  /**
   * Creates a definition for a Coding element with the specified name and cardinality.
   *
   * @param name The name of the element
   * @param cardinality The cardinality of the element
   * @return An {@link ElementDefinition} representing the Coding type
   */
  public static ElementDefinition createDefinition(
      @Nonnull final String name, final int cardinality) {
    return DefaultCompositeDefinition.of(
        name,
        List.of(
            DefaultPrimitiveDefinition.single("value", FHIRDefinedType.DECIMAL),
            DefaultPrimitiveDefinition.single("unit", FHIRDefinedType.STRING),
            DefaultPrimitiveDefinition.single("system", FHIRDefinedType.URI),
            DefaultPrimitiveDefinition.single("code", FHIRDefinedType.CODE)),
        cardinality,
        FHIRDefinedType.QUANTITY);
  }

  private static final ElementDefinition LITERAL_DEFINITION = createDefinition("", 1);

  /**
   * @param columnRepresentation The column representation to use
   * @param type The FHIRPath type
   * @param fhirType The FHIR type
   * @param definition The FHIR definition
   */
  public QuantityCollection(
      @Nonnull final ColumnRepresentation columnRepresentation,
      @Nonnull final Optional<FhirPathType> type,
      @Nonnull final Optional<FHIRDefinedType> fhirType,
      @Nonnull final Optional<? extends NodeDefinition> definition,
      @Nonnull final Optional<Column> extensionMapColumn) {
    super(columnRepresentation, type, fhirType, definition, extensionMapColumn);
  }

  /**
   * Returns a new instance with the specified columnCtx and definition.
   *
   * @param columnRepresentation The columnCtx to use
   * @param definition The definition to use
   * @return A new instance of {@link QuantityCollection}
   */
  @Nonnull
  public static QuantityCollection build(
      @Nonnull final ColumnRepresentation columnRepresentation,
      @Nonnull final Optional<? extends NodeDefinition> definition) {
    return new QuantityCollection(
        columnRepresentation,
        Optional.of(FhirPathType.QUANTITY),
        Optional.of(FHIRDefinedType.QUANTITY),
        definition,
        Optional.empty());
  }

  /**
   * Returns a new instance with the specified columnCtx and unknown definition.
   *
   * @param columnRepresentation The columnCtx to use
   * @return A new instance of {@link QuantityCollection}
   */
  @Nonnull
  public static QuantityCollection build(@Nonnull final ColumnRepresentation columnRepresentation) {
    return build(columnRepresentation, Optional.of(LITERAL_DEFINITION));
  }

  /**
   * Returns a new instance, parsed from a FHIRPath literal representing a calendar duration.
   *
   * @param quantityLiteral the FHIRPath representation of the literal
   * @return A new instance of {@link QuantityCollection}
   * @see <a href="https://hl7.org/fhirpath/#time-valued-quantities">Time-valued quantities</a>
   */
  @Nonnull
  public static QuantityCollection fromLiteral(@Nonnull final String quantityLiteral) {
    return QuantityCollection.build(
        new DefaultRepresentation(
            QuantityEncoding.encodeLiteral(FhirPathQuantity.parse(quantityLiteral))),
        Optional.of(LITERAL_DEFINITION));
  }

  /**
   * Returns a new instance, created from a numeric column representation (such as INTEGER or
   * DECIMAL). This is used to support implicit conversions from numeric to quantity.
   *
   * @param columnRepresentation the numeric column representation
   * @return A new instance of {@link QuantityCollection}
   */
  @Nonnull
  public static QuantityCollection fromNumeric(
      @Nonnull final ColumnRepresentation columnRepresentation) {
    // build the Quantity collection from a numeric (Decimal or Integer) representation
    // by using UCUM '1' unit.
    return QuantityCollection.build(
        columnRepresentation.transform(QuantityEncoding::encodeNumeric), Optional.empty());
  }

  /**
   * Converts this quantity to a string collection using element-wise transformation.
   *
   * @return StringCollection representation of this quantity collection
   */
  @Nonnull
  public StringCollection asStringCollection() {
    return map(r -> r.transformWithUdf(QuantityToLiteral.FUNCTION_NAME), StringCollection::build);
  }

  @Nonnull
  @Override
  public StringCollection asStringPath() {
    return asSingular()
        .map(r -> r.callUdf(QuantityToLiteral.FUNCTION_NAME), StringCollection::build);
  }

  @Override
  public @Nonnull Function<Numeric, Collection> getMathOperation(
      @Nonnull final Numeric.MathOperation operation) {
    throw new UnsupportedFhirPathFeatureError("Quantity math operations are not supported yet");
  }

  @Override
  public @Nonnull Collection negate() {
    throw new UnsupportedFhirPathFeatureError("Quantity math operations are not supported yet");
  }

  @Override
  @Nonnull
  public ColumnComparator getComparator() {
    return QuantityComparator.getInstance();
  }

  /**
   * Converts this quantity to the specified unit.
   *
   * @param targetUnit The target unit as a Collection (should be a StringCollection)
   * @return QuantityCollection with matching unit, or EmptyCollection if the conversion is not
   *     possible.
   */
  @Nonnull
  public Collection toUnit(@Nonnull final Collection targetUnit) {
    final Column unitColumn = targetUnit.getColumn().singular().getValue();
    return map(q -> new DefaultRepresentation(QuantityValue.of(q).toUnit(unitColumn)));
  }

  /**
   * Checks if this quantity can be converted to the specified unit. Follows the same rules as
   * FHIRPath convertibleToXXX functions, returning a BooleanCollection true/false value for
   * non-empty input collections or an empty boolean collection if the input is empty.
   *
   * @param targetUnit The target unit as a Collection (should be a StringCollection)
   * @return BooleanCollection indicating if conversion is possible
   */
  @Nonnull
  public Collection convertibleToUnit(@Nonnull final Collection targetUnit) {
    final Column unitColumn = targetUnit.getColumn().singular().getValue();
    final Column result = QuantityValue.of(getColumn()).convertibleToUnit(unitColumn);
    return BooleanCollection.build(new DefaultRepresentation(result));
  }
}
