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

package au.csiro.pathling.projection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import au.csiro.pathling.fhirpath.collection.CodingCollection;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.EmptyCollection;
import au.csiro.pathling.fhirpath.collection.StringCollection;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import au.csiro.pathling.fhirpath.encoding.CodingSchema;
import au.csiro.pathling.fhirpath.encoding.QuantityEncoding;
import au.csiro.pathling.fhirpath.path.Paths.Traversal;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.jupiter.api.Test;

class ProjectionResultTest {

  @Nonnull
  private static ProjectedColumn column(
      @Nonnull final String name,
      final boolean isCollection,
      @Nonnull final Optional<FHIRDefinedType> fhirType,
      @Nonnull final Optional<DataType> sqlType) {
    final RequestedColumn requested =
        new RequestedColumn(new Traversal(name), name, isCollection, fhirType, sqlType);
    return new ProjectedColumn(StringCollection.empty(), requested);
  }

  @Nonnull
  private static ProjectedColumn column(
      @Nonnull final Collection collection, @Nonnull final String name) {
    final RequestedColumn requested =
        new RequestedColumn(new Traversal(name), name, false, Optional.empty(), Optional.empty());
    return new ProjectedColumn(collection, requested);
  }

  @Nonnull
  private static ProjectionResult resultOf(@Nonnull final ProjectedColumn... columns) {
    return ProjectionResult.of(List.of(columns), org.apache.spark.sql.functions.lit(null));
  }

  @Test
  void usesExplicitSqlType() {
    final ProjectionResult result =
        resultOf(
            column(
                "amount",
                false,
                Optional.of(FHIRDefinedType.INTEGER),
                Optional.of(DataTypes.LongType)));
    final StructType schema = result.getSqlType();
    assertEquals(
        new StructType(
            new StructField[] {
              new StructField(
                  "amount", DataTypes.LongType, true, org.apache.spark.sql.types.Metadata.empty())
            }),
        schema);
  }

  @Test
  void usesExplicitFhirType() {
    final ProjectionResult result =
        resultOf(column("name", false, Optional.of(FHIRDefinedType.STRING), Optional.empty()));
    final StructType schema = result.getSqlType();
    assertEquals(DataTypes.StringType, schema.fields()[0].dataType());
    assertEquals("name", schema.fields()[0].name());
  }

  @Test
  void wrapsCollectionFieldsInArrayType() {
    final ProjectionResult result =
        resultOf(column("ids", true, Optional.of(FHIRDefinedType.INTEGER), Optional.empty()));
    final StructType schema = result.getSqlType();
    assertEquals(DataTypes.createArrayType(DataTypes.IntegerType), schema.fields()[0].dataType());
  }

  @Test
  void infersFromCollectionFhirType() {
    final Collection collection = StringCollection.fromValue("x");
    final ProjectionResult result = resultOf(column(collection, "code"));
    final StructType schema = result.getSqlType();
    assertEquals(DataTypes.StringType, schema.fields()[0].dataType());
  }

  @Test
  void preservesDeclarationOrderForMultipleColumns() {
    final ProjectionResult result =
        resultOf(
            column("linkId", false, Optional.of(FHIRDefinedType.STRING), Optional.empty()),
            column("count", false, Optional.of(FHIRDefinedType.INTEGER), Optional.empty()),
            column("active", true, Optional.of(FHIRDefinedType.BOOLEAN), Optional.empty()));
    final StructType schema = result.getSqlType();
    assertEquals(3, schema.fields().length);
    assertEquals("linkId", schema.fields()[0].name());
    assertEquals(DataTypes.StringType, schema.fields()[0].dataType());
    assertEquals("count", schema.fields()[1].name());
    assertEquals(DataTypes.IntegerType, schema.fields()[1].dataType());
    assertEquals("active", schema.fields()[2].name());
    assertEquals(DataTypes.createArrayType(DataTypes.BooleanType), schema.fields()[2].dataType());
  }

  @Test
  void handlesComplexFhirTypes() {
    final StructType codingSchema =
        resultOf(column("code", false, Optional.of(FHIRDefinedType.CODING), Optional.empty()))
            .getSqlType();
    assertEquals(CodingSchema.codingStructType(), codingSchema.fields()[0].dataType());

    final StructType quantitySchema =
        resultOf(column("amount", false, Optional.of(FHIRDefinedType.QUANTITY), Optional.empty()))
            .getSqlType();
    assertEquals(QuantityEncoding.dataType(), quantitySchema.fields()[0].dataType());
  }

  @Test
  void throwsWhenNoTypeInformationAvailable() {
    // EmptyCollection is Materializable but has no resolved FhirPathType.
    final Collection collection = EmptyCollection.getInstance();
    assertThrows(
        UnsupportedOperationException.class,
        () -> resultOf(column(collection, "value")).getSqlType());
  }

  @Test
  void throwsForNonMaterializableCollectionWithoutAnnotation() {
    // CodingCollection is not Materializable; without a declared type annotation it must throw.
    final Collection collection = CodingCollection.build(DefaultRepresentation.empty());
    assertThrows(
        UnsupportedOperationException.class,
        () -> resultOf(column(collection, "code")).getSqlType());
  }

  @Test
  void succeedsForNonMaterializableCollectionWithExplicitSqlType() {
    // An explicit sqlType annotation bypasses the Materializable check entirely.
    final ProjectedColumn col =
        new ProjectedColumn(
            CodingCollection.build(DefaultRepresentation.empty()),
            new RequestedColumn(
                new Traversal("code"),
                "code",
                false,
                Optional.empty(),
                Optional.of(DataTypes.StringType)));
    assertEquals(DataTypes.StringType, resultOf(col).getSqlType().fields()[0].dataType());
  }
}
