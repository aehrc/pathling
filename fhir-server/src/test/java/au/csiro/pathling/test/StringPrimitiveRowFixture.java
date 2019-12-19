package au.csiro.pathling.test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

public class StringPrimitiveRowFixture {

  public static StructType createPrimitiveRowStruct(DataType dataType) {
    Metadata metadata = new MetadataBuilder().build();
    StructField id = new StructField("123abcd_id", dataType, false, metadata);
    StructField value = new StructField("123abcd", dataType, true, metadata);
    return new StructType(new StructField[]{id, value});
  }


  private final static StructType STRING_SCHEMA = createPrimitiveRowStruct(DataTypes.StringType);


  public final static String STRING_ROW_ID_1 = "abc1";
  public final static String STRING_ROW_ID_2 = "abc2";
  public final static String STRING_ROW_ID_3 = "abc3";
  public final static String STRING_ROW_ID_4 = "abc4";
  public final static String STRING_ROW_ID_5 = "abc5";


  public final static Row STRING_1_JUDE = RowFactory.create("abc1", "Jude");
  public final static Row STRING_2_SAMUEL = RowFactory.create("abc2", "Samuel");
  public final static Row STRING_2_THOMAS = RowFactory.create("abc2", "Thomas");
  public final static Row STRING_3_NULL = RowFactory.create("abc3", null);
  public final static Row STRING_4_ADAM = RowFactory.create("abc4", "Adam");
  public final static Row STRING_5_NULL = RowFactory.create("abc5", null);


  public final static List<String> STRING_ALL_IDS = Arrays.asList(STRING_ROW_ID_1,
      STRING_ROW_ID_2, STRING_ROW_ID_3, STRING_ROW_ID_4, STRING_ROW_ID_5);

  public final static List<Row> STRING_ALL_ROWS = Arrays
      .asList(STRING_1_JUDE, STRING_2_SAMUEL, STRING_2_THOMAS, STRING_3_NULL,
          STRING_4_ADAM, STRING_4_ADAM, STRING_5_NULL, STRING_5_NULL);

  public final static List<Row> STRING_NULL_ROWS = Arrays
      .asList(STRING_3_NULL, STRING_5_NULL, STRING_5_NULL);

  public final static List<Row> NO_ROWS = Collections.emptyList();

  public final static List<Row> STRING_ALL_ROWS_NULL = STRING_ALL_IDS.stream()
      .map(s -> RowFactory.create(s, null)).collect(Collectors.toList());

  public static Dataset<Row> createCompleteDataset(SparkSession spark) {
    return spark.createDataFrame(STRING_ALL_ROWS, STRING_SCHEMA);
  }

  public static Dataset<Row> createDataset(SparkSession spark, Row... rows) {
    return spark.createDataFrame(Arrays.asList(rows), STRING_SCHEMA);
  }

  public static Dataset<Row> createNullRowsDataset(SparkSession spark) {
    return spark.createDataFrame(STRING_NULL_ROWS, STRING_SCHEMA);
  }

  public static Dataset<Row> createAllRowsNullDataset(SparkSession spark) {
    return spark.createDataFrame(STRING_ALL_ROWS_NULL, STRING_SCHEMA);
  }


  public static Dataset<Row> createEmptyDataset(SparkSession spark) {
    return spark.createDataFrame(NO_ROWS, STRING_SCHEMA);
  }

}
