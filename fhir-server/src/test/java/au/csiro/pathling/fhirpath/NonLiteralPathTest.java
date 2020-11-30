package au.csiro.pathling.fhirpath;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.builders.ElementPathBuilder;
import java.util.Arrays;
import java.util.Collections;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Test some basic NonLiteralPath behaviour
 *
 * @author Piotr Szul
 */
@Tag("UnitTest")
public class NonLiteralPathTest {

  @Test
  public void testSingularNonLiteralEidExpansion() {
    // Check the result.
    final Dataset<Row> inputDataset = new DatasetBuilder()
        .withIdColumn()
        .withColumn(DataTypes.StringType)
        .withRow("Patient/abc1", "Jude")   // when: "two values"  expect: "Jude"
        .build();

    final ElementPath testPath = new ElementPathBuilder()
        .fhirType(FHIRDefinedType.STRING)
        .dataset(inputDataset)
        .idAndValueColumns()
        .expression("Patient.name")
        .singular(true)
        .build();

    final Column newNonNullEid = testPath
        .expandEid(functions.lit(2));
    assertEquals(Collections.singletonList(2), inputDataset.select(newNonNullEid).first().getList(0));

    final Column newNullEid = testPath
        .expandEid(functions.lit(null));
    assertNull(inputDataset.select(newNullEid).first().getList(0));
  }

  @Test
  public void testNonSingulaNonLiteralEidExpansion() {
    // Check the result.
    final Dataset<Row> inputDataset = new DatasetBuilder()
        .withIdColumn()
        .withEidColumn()
        .withColumn(DataTypes.StringType)
        .withRow("Patient/abc1", DatasetBuilder.makeEid(2, 3), "Adam")
        .withRow("Patient/abc1", DatasetBuilder.makeEid(1, 3), "Jude")
        .withRow("Patient/abc2", null, null)
        .build();

    final ElementPath testPath = new ElementPathBuilder()
        .fhirType(FHIRDefinedType.STRING)
        .dataset(inputDataset)
        .idAndEidAndValueColumns()
        .expression("Patient.name")
        .singular(false)
        .build();

    final Column idCol = testPath.getIdColumn();
    final Dataset<Row> pathDataset = testPath.getOrderedDataset();

    // test non null eid
    final Column newNonNullEid = testPath
        .expandEid(functions.lit(2));
    assertEquals(Arrays.asList(1, 3, 2),
        pathDataset.where(idCol.equalTo("Patient/abc1")).select(newNonNullEid).first().getList(0));
    assertNull(
        pathDataset.where(idCol.equalTo("Patient/abc2")).select(newNonNullEid).first().getList(0));

    // test  null eid
    final Column newNullEid = testPath
        .expandEid(functions.lit(null));

    assertNull(
        pathDataset.where(idCol.equalTo("Patient/abc1")).select(newNullEid).first().getList(0));
    assertNull(
        pathDataset.where(idCol.equalTo("Patient/abc2")).select(newNullEid).first().getList(0));
  }
}
