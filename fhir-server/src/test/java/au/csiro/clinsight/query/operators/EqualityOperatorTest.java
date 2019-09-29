package au.csiro.clinsight.query.operators;

import org.apache.spark.sql.SparkSession;

/**
 * @author John Grimes
 */
public class EqualityOperatorTest {

  private SparkSession spark;

  // public void setUp() {
  //   spark = SparkSession.builder()
  //       .appName("clinsight-test")
  //       .config("spark.master", "local")
  //       .getOrCreate();
  // }
  //
  // @Test
  // public void codeEqualsLiteralString() {
  //   Metadata metadata = new MetadataBuilder().build();
  //   StructField id = new StructField("123abcd_id", DataTypes.StringType, false, metadata);
  //   StructField value = new StructField("123abcd", DataTypes.StringType, true, metadata);
  //   StructType rowStruct = new StructType(new StructField[]{id, value});
  //
  //   Row row = RowFactory.create("abc", "female");
  //   Dataset<Row> leftDataset = spark.createDataFrame(Collections.singletonList(row), rowStruct);
  //
  // }
}