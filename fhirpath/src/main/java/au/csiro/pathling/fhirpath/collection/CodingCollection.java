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

import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.struct;

import au.csiro.pathling.fhirpath.FhirPathType;
import au.csiro.pathling.fhirpath.StringCoercible;
import au.csiro.pathling.fhirpath.TerminologyConcepts;
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import au.csiro.pathling.fhirpath.comparison.CodingComparator;
import au.csiro.pathling.fhirpath.comparison.ColumnComparator;
import au.csiro.pathling.fhirpath.comparison.Comparable;
import au.csiro.pathling.fhirpath.definition.ElementDefinition;
import au.csiro.pathling.fhirpath.definition.NodeDefinition;
import au.csiro.pathling.fhirpath.definition.defaults.DefaultCompositeDefinition;
import au.csiro.pathling.fhirpath.definition.defaults.DefaultPrimitiveDefinition;
import au.csiro.pathling.fhirpath.literal.CodingLiteral;
import au.csiro.pathling.sql.misc.CodingToLiteral;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Represents a collection of Coding-typed elements.
 *
 * @author John Grimes
 */
public class CodingCollection extends Collection implements Comparable, StringCoercible {

  private static final ColumnComparator COMPARATOR = new CodingComparator();

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
            DefaultPrimitiveDefinition.single("id", FHIRDefinedType.STRING),
            DefaultPrimitiveDefinition.single("system", FHIRDefinedType.URI),
            DefaultPrimitiveDefinition.single("version", FHIRDefinedType.STRING),
            DefaultPrimitiveDefinition.single("code", FHIRDefinedType.CODE),
            DefaultPrimitiveDefinition.single("display", FHIRDefinedType.STRING),
            DefaultPrimitiveDefinition.single("userSelected", FHIRDefinedType.BOOLEAN)
        ),
        cardinality,
        FHIRDefinedType.CODING
    );
  }

  private static final ElementDefinition LITERAL_DEFINITION =
      createDefinition("", 1);


  /**
   * Creates a new CodingCollection.
   *
   * @param columnRepresentation the column representation for this collection
   * @param type the FhirPath type
   * @param fhirType the FHIR type
   * @param definition the node definition
   * @param extensionMapColumn the extension map column
   */
  protected CodingCollection(@Nonnull final ColumnRepresentation columnRepresentation,
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
   * @return A new instance of {@link CodingCollection}
   */
  @Nonnull
  public static CodingCollection build(@Nonnull final ColumnRepresentation columnRepresentation,
      @Nonnull final Optional<? extends NodeDefinition> definition) {
    return new CodingCollection(columnRepresentation, Optional.of(FhirPathType.CODING),
        Optional.of(FHIRDefinedType.CODING), definition, Optional.empty());
  }


  /**
   * Returns a new instance with the specified column and no definition.
   *
   * @param columnRepresentation The column to use
   * @return A new instance of {@link CodingCollection}
   */
  @Nonnull
  public static CodingCollection build(@Nonnull final ColumnRepresentation columnRepresentation) {
    return build(columnRepresentation, Optional.of(LITERAL_DEFINITION));
  }


  /**
   * Returns a new instance, parsed from a FHIRPath literal.
   *
   * @param fhirPath The FHIRPath representation of the literal
   * @return A new instance of {@link CodingCollection}
   * @throws IllegalArgumentException if the literal is malformed
   */
  @Nonnull
  public static CodingCollection fromLiteral(@Nonnull final String fhirPath)
      throws IllegalArgumentException {
    final Coding coding = CodingLiteral.fromString(fhirPath);
    final Column column = buildColumn(coding);
    return CodingCollection.build(new DefaultRepresentation(column));
  }

  @Nonnull
  private static Column buildColumn(@Nonnull final Coding coding) {
    return struct(
        lit(coding.getId()).cast(DataTypes.StringType).as("id"),
        lit(coding.getSystem()).cast(DataTypes.StringType).as("system"),
        lit(coding.getVersion()).cast(DataTypes.StringType).as("version"),
        lit(coding.getCode()).cast(DataTypes.StringType).as("code"),
        lit(coding.getDisplay()).cast(DataTypes.StringType).as("display"),
        lit(coding.hasUserSelected()
            ? coding.getUserSelected()
            : null).cast(DataTypes.BooleanType).as("userSelected"),
        lit(null).cast(DataTypes.IntegerType).as("_fid"));
  }

  @Override
  public boolean isComparableTo(@Nonnull final Comparable path) {
    return path instanceof CodingCollection;
  }

  @Nonnull
  @Override
  public ColumnComparator getComparator() {
    return COMPARATOR;
  }

  @Override
  @Nonnull
  public Optional<TerminologyConcepts> toConcepts() {
    return Optional.of(TerminologyConcepts.set(this.getColumn(), this));
  }

  @Nonnull
  @Override
  public StringCollection asStringPath() {
    return asSingular()
        .map(c -> c.transformWithUdf(CodingToLiteral.FUNCTION_NAME), StringCollection::build);
  }
}
