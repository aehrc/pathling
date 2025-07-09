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

import au.csiro.pathling.fhirpath.FhirPathType;
import au.csiro.pathling.fhirpath.StringCoercible;
import au.csiro.pathling.fhirpath.annotations.SqlPrimitive;
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import au.csiro.pathling.fhirpath.definition.NodeDefinition;
import au.csiro.pathling.fhirpath.operator.Comparable;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import org.apache.spark.sql.Column;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Represents a collection of Boolean-typed elements.
 *
 * @author John Grimes
 */
@SqlPrimitive
public class BooleanCollection extends Collection implements Comparable, StringCoercible {

  protected BooleanCollection(@Nonnull final ColumnRepresentation columnRepresentation,
      @Nonnull final Optional<FhirPathType> type,
      @Nonnull final Optional<FHIRDefinedType> fhirType,
      @Nonnull final Optional<? extends NodeDefinition> definition,
      @Nonnull final Optional<Column> extensionMapColumn) {
    super(columnRepresentation, type, fhirType, definition, extensionMapColumn);
  }

  /**
   * Returns a new instance with the specified column and definition.
   *
   * @param columnRepresentation The column to use
   * @param definition The definition to use
   * @return A new instance of {@link BooleanCollection}
   */
  @Nonnull
  public static BooleanCollection build(@Nonnull final ColumnRepresentation columnRepresentation,
      @Nonnull final Optional<NodeDefinition> definition) {
    return new BooleanCollection(columnRepresentation, Optional.of(FhirPathType.BOOLEAN),
        Optional.of(FHIRDefinedType.BOOLEAN), definition, Optional.empty());
  }

  /**
   * Returns a new instance with the specified column and no definition.
   *
   * @param value The column to use
   * @return A new instance of {@link BooleanCollection}
   */
  @Nonnull
  public static BooleanCollection build(@Nonnull final ColumnRepresentation value) {
    return BooleanCollection.build(value, Optional.empty());
  }

  /**
   * Returns a new instance, parsed from a FHIRPath literal.
   *
   * @param literal The FHIRPath representation of the literal
   * @return A new instance of {@link BooleanCollection}
   */
  @Nonnull
  public static BooleanCollection fromLiteral(@Nonnull final String literal) {
    return BooleanCollection.fromValue(literal.equals("true"));
  }

  /**
   * Returns a new instance based upon a literal represented by a {@link BooleanType}.
   * <p>
   * This is required for the reflection-based instantiation of collections used in
   * {@link au.csiro.pathling.view.ProjectionContext#of}.
   *
   * @param value The value to use
   * @return A new instance of {@link BooleanCollection}
   */
  @SuppressWarnings("unused")
  @Nonnull
  public static BooleanCollection fromValue(@Nonnull final BooleanType value) {
    return BooleanCollection.fromValue(value.getValue());
  }

  /**
   * Returns a new instance based upon a literal value.
   *
   * @param value The value to use
   * @return A new instance of {@link BooleanCollection}
   */
  @Nonnull
  public static BooleanCollection fromValue(final boolean value) {
    return BooleanCollection.build(DefaultRepresentation.literal(value));
  }

  /**
   * Returns an empty boolean collection.
   *
   * @return A new instance of {@link BooleanCollection}
   */
  @Nonnull
  public static BooleanCollection empty() {
    return build(DefaultRepresentation.empty());
  }

  /**
   * {@inheritDoc}
   * <p>
   * This implementation returns a new instance with the specified column representation.
   */
  @Override
  public boolean isComparableTo(@Nonnull final Comparable path) {
    return path instanceof BooleanCollection || Comparable.super.isComparableTo(path);
  }

  /**
   * {@inheritDoc}
   * <p>
   * This implementation returns true, as BooleanCollection can be converted to any other collection
   * type.
   */
  @Override
  @Nonnull
  public StringCollection asStringPath() {
    return Collection.defaultAsStringPath(this);
  }

  /**
   * {@inheritDoc}
   * <p>
   * This implementation returns itself as it is already a BooleanCollection.
   */
  @Override
  @Nonnull
  public BooleanCollection asBooleanPath() {
    return this;
  }

  /**
   * {@inheritDoc}
   * <p>
   * Special case for Boolean collection to go with the specialized `asBooleanPath()`.
   */
  @Override
  @Nonnull
  public BooleanCollection asBooleanSingleton() {
    return (BooleanCollection) this.asSingular();
  }

}
