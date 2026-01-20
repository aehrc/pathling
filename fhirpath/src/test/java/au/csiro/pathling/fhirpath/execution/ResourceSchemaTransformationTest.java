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

package au.csiro.pathling.fhirpath.execution;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.datasource.ObjectDataSource;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.r4.model.Enumerations.AdministrativeGender;
import org.hl7.fhir.r4.model.Patient;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Tests for {@link ResourceSchemaTransformation}.
 */
@SpringBootUnitTest
class ResourceSchemaTransformationTest {

  @Autowired
  SparkSession spark;

  @Autowired
  FhirEncoders encoders;

  @Test
  void wrapToEvaluationSchema_createsCorrectStructure() {
    // Create a simple test dataset
    final Dataset<Row> flatDataset = createPatientDataSource().read("Patient");

    // Wrap to evaluation schema
    final Dataset<Row> wrapped = ResourceSchemaTransformation.wrapToEvaluationSchema(
        "Patient", flatDataset);

    // Verify the schema structure
    final String[] columnNames = wrapped.columns();
    assertEquals(3, columnNames.length);
    assertTrue(Set.of(columnNames).contains("id"));
    assertTrue(Set.of(columnNames).contains("key"));
    assertTrue(Set.of(columnNames).contains("Patient"));

    // Verify the Patient column is a struct type
    final DataType patientColumnType = wrapped.schema().apply("Patient").dataType();
    assertInstanceOf(StructType.class, patientColumnType);

    // Verify the nested struct contains the original columns
    final StructType nestedStruct = (StructType) patientColumnType;
    final Set<String> nestedColumnNames = Arrays.stream(nestedStruct.fieldNames())
        .collect(Collectors.toSet());
    assertTrue(nestedColumnNames.contains("id"));
    assertTrue(nestedColumnNames.contains("gender"));
    assertTrue(nestedColumnNames.contains("id_versioned"));
  }

  @Test
  void wrapToEvaluationSchema_preservesData() {
    final Dataset<Row> flatDataset = createPatientDataSource().read("Patient");
    final long originalCount = flatDataset.count();

    // Wrap and verify count is preserved
    final Dataset<Row> wrapped = ResourceSchemaTransformation.wrapToEvaluationSchema(
        "Patient", flatDataset);
    assertEquals(originalCount, wrapped.count());

    // Verify ID values are preserved
    final List<String> originalIds = flatDataset.select("id").collectAsList()
        .stream().map(r -> r.getString(0)).sorted().toList();
    final List<String> wrappedIds = wrapped.select("id").collectAsList()
        .stream().map(r -> r.getString(0)).sorted().toList();
    assertEquals(originalIds, wrappedIds);
  }

  @Test
  void unwrapToFlatSchema_restoresOriginalStructure() {
    final Dataset<Row> originalDataset = createPatientDataSource().read("Patient");
    final String[] originalColumns = originalDataset.columns();

    // Wrap and then unwrap
    final Dataset<Row> wrapped = ResourceSchemaTransformation.wrapToEvaluationSchema(
        "Patient", originalDataset);
    final Dataset<Row> unwrapped = ResourceSchemaTransformation.unwrapToFlatSchema(
        "Patient", wrapped);

    // Verify columns match
    assertArrayEquals(originalColumns, unwrapped.columns());

    // Verify data is preserved
    assertEquals(originalDataset.count(), unwrapped.count());
  }

  @Test
  void wrapAndUnwrap_isIdempotent() {
    final Dataset<Row> originalDataset = createPatientDataSource().read("Patient");

    // Perform wrap -> unwrap
    final Dataset<Row> wrapped = ResourceSchemaTransformation.wrapToEvaluationSchema(
        "Patient", originalDataset);
    final Dataset<Row> unwrapped = ResourceSchemaTransformation.unwrapToFlatSchema(
        "Patient", wrapped);

    // Verify the data is equivalent (same rows)
    final List<Row> originalRows = originalDataset.orderBy("id").collectAsList();
    final List<Row> unwrappedRows = unwrapped.orderBy("id").collectAsList();

    assertEquals(originalRows.size(), unwrappedRows.size());

    // Schema should be identical
    assertEquals(originalDataset.schema(), unwrapped.schema());
  }

  @Test
  void wrapToEvaluationSchema_keyColumn_containsVersionedId() {
    final Dataset<Row> flatDataset = createPatientDataSource().read("Patient");

    // Wrap to evaluation schema
    final Dataset<Row> wrapped = ResourceSchemaTransformation.wrapToEvaluationSchema(
        "Patient", flatDataset);

    // Verify key column type
    final DataType keyColumnType = wrapped.schema().apply("key").dataType();
    assertEquals(DataTypes.StringType, keyColumnType);

    // Verify key values match id_versioned values from the nested struct
    final List<String> keyValues = wrapped.select("key").collectAsList()
        .stream().map(r -> r.getString(0)).sorted().toList();
    final List<String> idVersionedValues = wrapped.select("Patient.id_versioned").collectAsList()
        .stream().map(r -> r.getString(0)).sorted().toList();

    assertEquals(idVersionedValues, keyValues);
  }

  private ObjectDataSource createPatientDataSource() {
    final Patient patient1 = new Patient();
    patient1.setId("1");
    patient1.setGender(AdministrativeGender.MALE);

    final Patient patient2 = new Patient();
    patient2.setId("2");
    patient2.setGender(AdministrativeGender.FEMALE);

    return new ObjectDataSource(spark, encoders, List.of(patient1, patient2));
  }
}
