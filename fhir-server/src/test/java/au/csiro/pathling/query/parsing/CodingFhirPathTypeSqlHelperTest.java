package au.csiro.pathling.query.parsing;

import static au.csiro.pathling.test.Assertions.assertThat;

import au.csiro.pathling.TestUtilities;
import au.csiro.pathling.test.CodingRowFixture;
import au.csiro.pathling.test.DatasetAssert;
import java.util.Arrays;
import java.util.function.BiFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.hl7.fhir.r4.model.Coding;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(au.csiro.pathling.UnitTest.class)
public class CodingFhirPathTypeSqlHelperTest {

  protected SparkSession spark;

  @Before
  public void setUp() {
    spark = TestUtilities.getSparkSession();
  }

  private BiFunction<Column, Column, Column> equality =
      CodingFhirPathTypeSqlHelper.INSTANCE.getEquality();

  private DatasetAssert assertEquality(Dataset<Row> dataset) {
    return assertThat(
        dataset.select(equality.apply(dataset.col("left"), dataset.col("right")).alias("result")));
  }

  @Test
  public void testEquality() {
    Metadata metadata = new MetadataBuilder().build();
    StructType codingStruct = CodingRowFixture.createCodingStruct(metadata);
    StructType schema =
        new StructType(new StructField[]{new StructField("left", codingStruct, true, metadata),
            new StructField("right", codingStruct, true, metadata)});

    Dataset<Row> trueDataset = spark.createDataFrame(
        Arrays.asList(
            RowFactory.create(RowFactory.create("id1", "system1", null, "code1", "display1", false),
                RowFactory.create("id2", "system1", null, "code1", "display2", true)),
            RowFactory.create(
                RowFactory.create("id1", "system1", "version1", "code1", "display1", false),
                RowFactory.create("id2", "system1", null, "code1", "display2", true)),
            RowFactory.create(
                RowFactory.create("id1", "system1", "version1", "code1", "display1", false),
                RowFactory.create("id2", "system1", "version1", "code1", "display2", true))),
        schema);

    assertEquality(trueDataset).isValues().hasSize(3).containsOnly(true);

    Dataset<Row> falseDataset = spark.createDataFrame(Arrays.asList(
        RowFactory.create(
            RowFactory.create("id1", "system1", "version1", "code1", "display1", false),
            RowFactory.create("id2", "system1", "version2", "code1", "display2", true)),
        RowFactory.create(RowFactory.create("id1", "system1", null, "code1", "display1", true),
            RowFactory.create("id1", "system1", null, "code2", "display1", true)),
        RowFactory.create(RowFactory.create("id1", "system1", null, "code1", "display1", true),
            RowFactory.create("id1", "system2", null, "code1", "display1", true))),
        schema);

    assertEquality(falseDataset).isValues().hasSize(3).containsOnly(false);
  }

  @Test
  public void testLiteral() {
    Metadata metadata = new MetadataBuilder().build();
    StructType schema = new StructType(
        new StructField[]{new StructField("id", DataTypes.StringType, true, metadata)});
    Dataset<Row> context = spark.createDataFrame(Arrays.asList(RowFactory.create("id")), schema);

    Coding emptyCoding = new Coding();
    Column emptyLitColumn = CodingFhirPathTypeSqlHelper.INSTANCE.getLiteralColumn(emptyCoding);
    assertThat(context.select(emptyLitColumn.alias("literal"))).isValue()
        .isEqualTo(RowFactory.create(null, null, null, null, null, false));

    Coding fullCoding = new Coding();
    fullCoding.setId("id");
    fullCoding.setSystem("system");
    fullCoding.setVersion("version");
    fullCoding.setCode("code");
    fullCoding.setDisplay("display");
    fullCoding.setUserSelected(true);
    Column fullLitCoding = CodingFhirPathTypeSqlHelper.INSTANCE.getLiteralColumn(fullCoding);
    assertThat(context.select(fullLitCoding.alias("literal"))).isValue()
        .isEqualTo(RowFactory.create("id", "system", "version", "code", "display", true));
  }

}
