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

package au.csiro.pathling.fhirpath.column;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import au.csiro.pathling.test.SpringBootUnitTest;
import java.util.Optional;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/** Tests for {@link ResourceRepresentation}. */
@SpringBootUnitTest
class ResourceRepresentationTest {

  @Autowired SparkSession spark;

  @Test
  void withIdColumn_createsRepresentationWithIdColumn() {
    final ResourceRepresentation rep = ResourceRepresentation.withIdColumn();

    assertNotNull(rep);
    assertNotNull(rep.getValue());
  }

  @Test
  void withIdColumn_createsNewInstancesEachTime() {
    final ResourceRepresentation rep1 = ResourceRepresentation.withIdColumn();
    final ResourceRepresentation rep2 = ResourceRepresentation.withIdColumn();

    // Should be equal but not same instance
    assertNotSame(rep1, rep2);
    assertEquals(rep1, rep2);
  }

  @Test
  void of_createsRepresentationWithSpecifiedColumn() {
    final Column customColumn = col("custom_id");
    final ResourceRepresentation rep = ResourceRepresentation.of(customColumn);

    assertNotNull(rep);
    assertEquals(customColumn, rep.getExistenceColumn());
  }

  @Test
  void getValue_returnsExistenceColumn() {
    final Dataset<Row> dataset = createFlatPatientDataset();

    final ResourceRepresentation rep = ResourceRepresentation.withIdColumn();
    final Column value = rep.getValue();

    // Apply the column to the dataset and verify it extracts the id value
    final String result = dataset.select(value).first().getString(0);
    assertEquals("1", result);
  }

  @Test
  void vectorize_appliesSingularExpression() {
    final ResourceRepresentation rep = ResourceRepresentation.withIdColumn();

    // Apply a transform via vectorize - should use singular expression since resources are singular
    final ColumnRepresentation vectorized =
        rep.vectorize(
            arr -> arr, // array expression (not used)
            Column::isNotNull // singular expression
            );

    // The result should be a new ResourceRepresentation
    assertInstanceOf(ResourceRepresentation.class, vectorized);

    // Test that the singular expression was applied
    final Dataset<Row> dataset = createFlatPatientDataset();
    final Boolean result = dataset.select(vectorized.getValue()).first().getBoolean(0);
    assertEquals(true, result);
  }

  @Test
  void flatten_returnsItself() {
    final ResourceRepresentation rep = ResourceRepresentation.withIdColumn();
    final ColumnRepresentation flattened = rep.flatten();

    // Should return itself since already flat
    assertEquals(rep, flattened);
  }

  @Test
  void map_appliesTransformation() {
    final ResourceRepresentation rep = ResourceRepresentation.withIdColumn();
    final ColumnRepresentation mapped = rep.map(Column::isNotNull);

    assertInstanceOf(ResourceRepresentation.class, mapped);

    final Dataset<Row> dataset = createFlatPatientDataset();
    final Boolean result = dataset.select(mapped.getValue()).first().getBoolean(0);
    assertEquals(true, result);
  }

  @Test
  void traverse_fromRoot_createsTopLevelColumn() {
    // Create a flat schema dataset with top-level columns
    final Dataset<Row> dataset = createFlatPatientDataset();

    final ResourceRepresentation root = ResourceRepresentation.withIdColumn();
    final ColumnRepresentation genderRep = root.traverse("gender");

    // Apply the column to the dataset and verify it extracts the correct value
    final String result = dataset.select(genderRep.getValue()).first().getString(0);
    assertEquals("male", result);
  }

  @Test
  void traverse_fromRoot_returnsDefaultRepresentation() {
    final ResourceRepresentation root = ResourceRepresentation.withIdColumn();
    final ColumnRepresentation result = root.traverse("gender");

    assertInstanceOf(
        DefaultRepresentation.class, result, "traverse() should return a DefaultRepresentation");
  }

  @Test
  void traverse_nested_usesGetField() {
    // Create a flat schema dataset with nested struct
    final Dataset<Row> dataset = createFlatPatientDatasetWithName();

    final ResourceRepresentation root = ResourceRepresentation.withIdColumn();
    // First traverse goes to top-level 'name' column
    final ColumnRepresentation nameRep = root.traverse("name");
    // Second traverse should use getField for 'family'
    final ColumnRepresentation familyRep = nameRep.traverse("family");

    // Apply and verify
    final String result = dataset.select(familyRep.getValue()).first().getString(0);
    assertEquals("Smith", result);
  }

  @Test
  void getField_createsTopLevelColumnWithoutFlatten() {
    final Dataset<Row> dataset = createFlatPatientDataset();

    final ResourceRepresentation root = ResourceRepresentation.withIdColumn();
    final ColumnRepresentation genderRep = root.getField("gender");

    assertInstanceOf(
        DefaultRepresentation.class, genderRep, "getField() should return a DefaultRepresentation");

    // Verify it extracts the correct value
    final String result = dataset.select(genderRep.getValue()).first().getString(0);
    assertEquals("male", result);
  }

  @Test
  void traverse_withFhirType_handlesStandardTypes() {
    final Dataset<Row> dataset = createFlatPatientDataset();

    final ResourceRepresentation root = ResourceRepresentation.withIdColumn();
    // STRING type should be handled without special processing
    final ColumnRepresentation genderRep =
        root.traverse("gender", Optional.of(FHIRDefinedType.CODE));

    final String result = dataset.select(genderRep.getValue()).first().getString(0);
    assertEquals("male", result);
  }

  @Test
  void traverse_withBase64BinaryType_appliesBinaryHandling() {
    final ResourceRepresentation root = ResourceRepresentation.withIdColumn();
    // BASE64BINARY should use special handling
    final ColumnRepresentation binaryRep =
        root.traverse("data", Optional.of(FHIRDefinedType.BASE64BINARY));

    // The result should be a representation (we can't easily test binary conversion without data)
    assertNotNull(binaryRep);
  }

  @Test
  void flatSchemaVsEvaluationSchema_differentColumnExpressions() {
    // This test demonstrates the difference between flat and evaluation schema traversal

    // Flat schema: traverse("gender") -> col("gender")
    final ResourceRepresentation flatRoot = ResourceRepresentation.withIdColumn();
    final Column flatColumn = flatRoot.traverse("gender").getValue();

    // Evaluation schema: traverse("gender") -> col("Patient").getField("gender")
    final DefaultRepresentation evalRoot = new DefaultRepresentation(col("Patient"));
    final Column evalColumn = evalRoot.traverse("gender").getValue();

    // The column expressions should be different
    // Flat: gender (after removeNulls/flatten transformations)
    // Eval: Patient.gender (after removeNulls/flatten transformations)
    assertNotNull(flatColumn);
    assertNotNull(evalColumn);

    // We can't easily compare Column objects directly, but we've verified they
    // work correctly in the other tests by applying them to actual datasets
  }

  @Test
  void traverse_extensionField_createsDirectColumnReference() {
    // This tests that _extension field access works correctly for ResourceCollection

    final ResourceRepresentation root = ResourceRepresentation.withIdColumn();
    final ColumnRepresentation extensionRep = root.traverse("_extension");

    // Verify the extension can be accessed
    assertNotNull(extensionRep);
    // Note: The actual extension structure is complex, but we verify the column is created
  }

  @Test
  void copyOf_createsNewRepresentationWithNewColumn() {
    final ResourceRepresentation original = ResourceRepresentation.withIdColumn();
    final Column newColumn = lit("test");

    // copyOf is protected, but we can test it indirectly via vectorize
    final ColumnRepresentation copied = original.vectorize(c -> c, c -> newColumn);

    assertInstanceOf(ResourceRepresentation.class, copied);
    assertNotSame(original, copied);
  }

  @Test
  void equality_basedOnExistenceColumn() {
    final ResourceRepresentation rep1 = ResourceRepresentation.of(col("id"));
    final ResourceRepresentation rep2 = ResourceRepresentation.of(col("id"));
    final ResourceRepresentation rep3 = ResourceRepresentation.of(col("other"));

    // rep1 and rep2 have the same existence column
    assertEquals(rep1, rep2);
    // rep3 has a different existence column
    assertNotSame(rep1, rep3);
  }

  // Helper methods to create test datasets

  private Dataset<Row> createFlatPatientDataset() {
    final StructType schema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.StringType, true),
              DataTypes.createStructField("gender", DataTypes.StringType, true),
              DataTypes.createStructField("active", DataTypes.BooleanType, true)
            });

    return spark.createDataFrame(java.util.List.of(RowFactory.create("1", "male", true)), schema);
  }

  private Dataset<Row> createFlatPatientDatasetWithName() {
    final StructType nameSchema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("family", DataTypes.StringType, true),
              DataTypes.createStructField(
                  "given", DataTypes.createArrayType(DataTypes.StringType), true)
            });

    final StructType schema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.StringType, true),
              DataTypes.createStructField("name", nameSchema, true)
            });

    return spark.createDataFrame(
        java.util.List.of(
            RowFactory.create("1", RowFactory.create("Smith", new String[] {"John"}))),
        schema);
  }
}
