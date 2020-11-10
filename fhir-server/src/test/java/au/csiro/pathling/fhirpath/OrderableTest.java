package au.csiro.pathling.fhirpath;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.literal.StringLiteralPath;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.builders.ElementPathBuilder;
import java.util.Arrays;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

/**
 * Test some basic Orderable behaviour across different FhirPath types
 *
 * @author Piotr Szul
 */
@Tag("UnitTest")
public class OrderableTest {


  @Test
  public void testLiteralHasOrder() {

    // Check the result.
    final Dataset<Row> inputDataset = new DatasetBuilder()
        .withIdColumn()
        .withValueColumn(DataTypes.StringType)
        .withRow("Patient/abc1", "Jude")   // when: "two values"  expect: "Jude"
        .build();

    final ElementPath contextPath = new ElementPathBuilder()
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
  public void testSingularNonLiteralHasOrder() {
    // Check the result.
    final Dataset<Row> inputDataset = new DatasetBuilder()
        .withIdColumn()
        .withValueColumn(DataTypes.StringType)
        .withRow("Patient/abc1", "Jude")   // when: "two values"  expect: "Jude"
        .build();

    final ElementPath testPath = new ElementPathBuilder()
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

    final Column newNonNullEid = testPath
        .expandEid(functions.lit(2));
    assertEquals(Arrays.asList(2), inputDataset.select(newNonNullEid).first().getList(0));

    final Column newNullEid = testPath
        .expandEid(functions.lit(null));
    assertEquals(null, inputDataset.select(newNullEid).first().getList(0));
  }

  @Test
  public void testNonSingulaNonLiteralWithEidHasOrder() {
    // Check the result.
    final Dataset<Row> inputDataset = new DatasetBuilder()
        .withIdColumn()
        .withEidColumn()
        .withValueColumn(DataTypes.StringType)
        .withRow("Patient/abc1", DatasetBuilder.makeEid(2, 3), "Adam")
        .withRow("Patient/abc1", DatasetBuilder.makeEid(1, 3), "Jude")
        .withRow("Patient/abc2", null, null)
        .build();

    final ElementPath testPath = new ElementPathBuilder()
        .fhirType(FHIRDefinedType.STRING)
        .dataset(inputDataset)
        .idAndValueColumns()
        .eidColumn()
        .expression("Patient.name")
        .singular(false)
        .build();

    assertTrue(testPath.hasOrder());
    assertEquals(testPath.getEidColumn().get(), testPath.getOrderingColumn());
    testPath.checkHasOrder();

    final Column idCol = testPath.getIdColumn().get();
    final Dataset<Row> pathDataset = testPath.getOrderedDataset();

    // test non null eid
    final Column newNonNullEid = testPath
        .expandEid(functions.lit(2));
    assertEquals(Arrays.asList(1, 3, 2),
        pathDataset.where(idCol.equalTo("Patient/abc1")).select(newNonNullEid).first().getList(0));
    assertEquals(null,
        pathDataset.where(idCol.equalTo("Patient/abc2")).select(newNonNullEid).first().getList(0));

    // test  null eid
    final Column newNullEid = testPath
        .expandEid(functions.lit(null));

    assertEquals(null,
        pathDataset.where(idCol.equalTo("Patient/abc1")).select(newNullEid).first().getList(0));
    assertEquals(null,
        pathDataset.where(idCol.equalTo("Patient/abc2")).select(newNullEid).first().getList(0));
  }


  private static <T> void assertFailsOrderCheck(Executable e) {
    final IllegalStateException error = assertThrows(
        IllegalStateException.class,
        e);
    assertEquals(
        "Orderable path expected",
        error.getMessage());
  }

  @Test
  public void testNonSingularNonLiteralWithoutEidHasNoOrder() {
    // Check the result.
    final Dataset<Row> inputDataset = new DatasetBuilder()
        .withIdColumn()
        .withValueColumn(DataTypes.StringType)
        .withRow("Patient/abc1", "dam")
        .build();

    final ElementPath testPath = new ElementPathBuilder()
        .fhirType(FHIRDefinedType.STRING)
        .dataset(inputDataset)
        .idAndValueColumns()
        .expression("Patient.name")
        .singular(false)
        .build();

    assertFalse(testPath.hasOrder());
    assertFailsOrderCheck(() -> testPath.checkHasOrder());
    assertFailsOrderCheck(() -> testPath.getOrderedDataset());
    assertFailsOrderCheck(() -> testPath.getOrderingColumn());
  }
}
