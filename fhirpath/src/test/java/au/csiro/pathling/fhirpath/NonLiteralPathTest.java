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
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.builders.ElementPathBuilder;
import java.util.Arrays;
import java.util.Collections;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Tests some basic NonLiteralPath behaviour.
 *
 * @author Piotr Szul
 */
@SpringBootUnitTest
class NonLiteralPathTest {

  @Autowired
  SparkSession spark;

  @Test
  void testSingularNonLiteralEidExpansion() {
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

    final Column newNonNullEid = testPath
        .expandEid(functions.lit(2));
    assertEquals(Collections.singletonList(2),
        inputDataset.select(newNonNullEid).first().getList(0));

    final Column newNullEid = testPath
        .expandEid(functions.lit(null));
    assertTrue(inputDataset.select(newNullEid).first().isNullAt(0));
  }

  @Test
  void testNonSingularNonLiteralEidExpansion() {
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

    final Column idCol = testPath.getIdColumn();
    final Dataset<Row> pathDataset = testPath.getOrderedDataset();

    // Test non-null element ID.
    final Column newNonNullEid = testPath
        .expandEid(functions.lit(2));
    assertEquals(Arrays.asList(1, 3, 2),
        pathDataset.where(idCol.equalTo("patient-1")).select(newNonNullEid).first()
            .getList(0));
    assertTrue(
        pathDataset.where(idCol.equalTo("patient-2")).select(newNonNullEid).first()
            .isNullAt(0));

    // Test null element ID.
    final Column newNullEid = testPath
        .expandEid(functions.lit(null));

    assertEquals(Arrays.asList(1, 3, 0),
        pathDataset.where(idCol.equalTo("patient-1")).select(newNullEid).first()
            .getList(0));
    assertTrue(pathDataset.where(idCol.equalTo("patient-2")).select(newNullEid).first()
        .isNullAt(0));
  }


  @Test
  void testArrayExplode() {
    final Dataset<Row> inputDataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withColumn(DataTypes.StringType)
        .withRow("patient-1", DatasetBuilder.makeEid(2, 3), "Adam,Eve")
        .withRow("patient-1", DatasetBuilder.makeEid(1, 3), "Jude")
        .withRow("patient-2", DatasetBuilder.makeEid(0, 0), null)
        .withRow("patient-3", null, null)
        .build();

    final ElementPath testPath = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.STRING)
        .dataset(inputDataset)
        .idAndEidAndValueColumns()
        .expression("Patient.name")
        .singular(false)
        .build();

    final Dataset<Row> arrayDataset = testPath.getDataset()
        .withColumn("arrayCol", functions.split(testPath.getValueColumn(), ","));

    final MutablePair<Column, Column> valueAndEidColumns = new MutablePair<>();
    final Dataset<Row> explodedDataset = testPath
        .explodeArray(arrayDataset, arrayDataset.col("arrayCol"), valueAndEidColumns);

    final Dataset<Row> actualDataset = explodedDataset
        .select(testPath.getIdColumn(), valueAndEidColumns.getRight(), testPath.getValueColumn(),
            valueAndEidColumns.getLeft())
        .orderBy(testPath.getIdColumn(), valueAndEidColumns.getRight());

    final Dataset<Row> expectedDataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withColumn(DataTypes.StringType)
        .withColumn(DataTypes.StringType)
        .withRow("patient-1", DatasetBuilder.makeEid(1, 3, 0), "Jude", "Jude")
        .withRow("patient-1", DatasetBuilder.makeEid(2, 3, 0), "Adam,Eve", "Adam")
        .withRow("patient-1", DatasetBuilder.makeEid(2, 3, 1), "Adam,Eve", "Eve")
        .withRow("patient-2", DatasetBuilder.makeEid(0, 0, 0), null, null)
        .withRow("patient-3", null, null, null)
        .build();

    assertThat(actualDataset).hasRows(expectedDataset);
  }
}
