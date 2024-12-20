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
import au.csiro.pathling.fhirpath.Materializable;
import au.csiro.pathling.fhirpath.StringCoercible;
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import au.csiro.pathling.fhirpath.comparison.CodingComparator;
import au.csiro.pathling.fhirpath.definition.NodeDefinition;
import au.csiro.pathling.fhirpath.literal.CodingLiteral;
import au.csiro.pathling.fhirpath.operator.Comparable;
import au.csiro.pathling.sql.misc.CodingToLiteral;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import java.util.function.Function;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Represents a collection of Coding-typed elements.
 *
 * @author John Grimes
 */
public class CodingCollection extends Collection implements Materializable<Coding>,
    Comparable, StringCoercible {

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
      @Nonnull final Optional<NodeDefinition> definition) {
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
    return build(columnRepresentation, Optional.empty());
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

  @Nonnull
  @Override
  public Optional<Coding> getFhirValueFromRow(@Nonnull final Row row, final int columnNumber) {
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

  @Override
  public boolean isComparableTo(@Nonnull final Collection path) {
    return path instanceof CodingCollection || super.isComparableTo(path);
  }

  @Override
  @Nonnull
  public Function<Comparable, Column> getComparison(@Nonnull final ComparisonOperation operation) {
    return CodingComparator.buildComparison(this, operation);
  }

  @Nonnull
  @Override
  public Optional<CodingCollection> asCoding() {
    return Optional.of(this);
  }

  @Nonnull
  @Override
  public StringCollection asStringPath() {
    return map(c -> c.transformWithUdf(CodingToLiteral.FUNCTION_NAME), StringCollection::build);
  }

}
