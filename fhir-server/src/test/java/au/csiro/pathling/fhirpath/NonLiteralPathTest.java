/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.builders.ElementPathBuilder;
import java.util.Arrays;
import java.util.Collections;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * Tests some basic NonLiteralPath behaviour.
 *
 * @author Piotr Szul
 */
@SpringBootTest
@Tag("UnitTest")
public class NonLiteralPathTest {

  @Autowired
  private SparkSession spark;

  @Test
  public void testSingularNonLiteralEidExpansion() {
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
    assertNull(inputDataset.select(newNullEid).first().getList(0));
  }

  @Test
  public void testNonSingularNonLiteralEidExpansion() {
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
    assertNull(
        pathDataset.where(idCol.equalTo("patient-2")).select(newNonNullEid).first()
            .getList(0));

    // Test null element ID.
    final Column newNullEid = testPath
        .expandEid(functions.lit(null));

    assertEquals(Arrays.asList(1, 3, 0),
        pathDataset.where(idCol.equalTo("patient-1")).select(newNullEid).first()
            .getList(0));
    assertNull(pathDataset.where(idCol.equalTo("patient-2")).select(newNullEid).first()
        .getList(0));
  }
}
