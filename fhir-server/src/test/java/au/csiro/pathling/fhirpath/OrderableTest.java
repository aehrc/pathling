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

package au.csiro.pathling.fhirpath;

import static au.csiro.pathling.test.assertions.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.literal.StringLiteralPath;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.builders.ElementPathBuilder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * Test some basic Orderable behaviour across different FhirPath types.
 *
 * @author Piotr Szul
 */
@SpringBootTest
@Tag("UnitTest")
class OrderableTest {

  @Autowired
  SparkSession spark;

  @Test
  void testLiteralHasOrder() {

    // Check the result.
    final Dataset<Row> inputDataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withColumn(DataTypes.StringType)
        .withRow("patient-1", "Jude")   // when: "two values"  expect: "Jude"
        .build();

    final ElementPath contextPath = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.STRING)
        .dataset(inputDataset)
        .idAndValueColumns()
        .expression("Patient.name")
        .build();

    final StringLiteralPath testLiteralPath = StringLiteralPath.fromString("test", contextPath);

    assertTrue(testLiteralPath.hasOrder());
    assertEquals(testLiteralPath.getDataset(), testLiteralPath.getOrderedDataset());
    assertEquals(Orderable.ORDERING_NULL_VALUE, testLiteralPath.getOrderingColumn());
    testLiteralPath.checkHasOrder();
  }

  @Test
  void testSingularNonLiteralHasOrder() {
    // Check the result.
    final Dataset<Row> inputDataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withColumn(DataTypes.StringType)
        .withRow("patient-1", "Jude")   // when: "two values"  expect: "Jude"
        .build();

    final ElementPath testPath = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.STRING)
        .dataset(inputDataset)
        .idAndValueColumns()
        .expression("Patient.name")
        .singular(true)
        .build();

    assertTrue(testPath.hasOrder());
    assertEquals(testPath.getDataset(), testPath.getOrderedDataset());
    assertEquals(Orderable.ORDERING_NULL_VALUE, testPath.getOrderingColumn());
    testPath.checkHasOrder();

    final Dataset<Row> expectedDataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withColumn(DataTypes.StringType)
        .withRow("patient-1", null, "Jude")
        .build();

    assertThat(testPath)
        .selectOrderedResultWithEid()
        .hasRows(expectedDataset);
  }

  static void assertFailsOrderCheck(final Executable e) {
    final IllegalStateException error = assertThrows(IllegalStateException.class, e);
    assertEquals("Orderable path expected", error.getMessage());
  }

  @Test
  void testNonSingularNonLiteralWithEidHasOrder() {
    // Check the result.
    final Dataset<Row> inputDataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withColumn(DataTypes.StringType)
        .withRow("patient-1", DatasetBuilder.makeEid(2, 3), "Adam")
        .withRow("patient-1", DatasetBuilder.makeEid(1, 3), "Jude")
        .withRow("patient-2", null, null)
        .build();

    final ElementPath testPath = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.STRING)
        .dataset(inputDataset)
        .idAndEidAndValueColumns()
        .expression("Patient.name")
        .singular(false)
        .build();

    assertTrue(testPath.hasOrder());
    assertTrue(testPath.getEidColumn().isPresent());
    assertEquals(testPath.getEidColumn().get(), testPath.getOrderingColumn());
    testPath.checkHasOrder();

    final Dataset<Row> expectedDataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withColumn(DataTypes.StringType)
        .withRow("patient-1", DatasetBuilder.makeEid(1, 3), "Jude")
        .withRow("patient-1", DatasetBuilder.makeEid(2, 3), "Adam")
        .withRow("patient-2", null, null)
        .build();

    assertThat(testPath)
        .selectOrderedResultWithEid()
        .hasRows(expectedDataset);
  }

  @Test
  void testNonSingularNonLiteralWithoutEidHasNoOrder() {
    // Check the result.
    final Dataset<Row> inputDataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withColumn(DataTypes.StringType)
        .withRow("patient-1", "Adam")
        .build();

    final ElementPath testPath = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.STRING)
        .dataset(inputDataset)
        .idAndValueColumns()
        .expression("Patient.name")
        .singular(false)
        .build();

    assertFalse(testPath.hasOrder());
    assertFailsOrderCheck(testPath::checkHasOrder);
    assertFailsOrderCheck(testPath::getOrderedDataset);
    assertFailsOrderCheck(testPath::getOrderingColumn);
  }
}
