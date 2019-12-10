package au.csiro.clinsight.test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class StringPrimitiveRowFixture {	
	
	public static StructType createPrimitiveRowStruct(DataType dataType) {
		Metadata metadata = new MetadataBuilder().build();
		StructField id = new StructField("123abcd_id", dataType, false, metadata);
		StructField value = new StructField("123abcd", dataType, true, metadata);
		return new StructType(new StructField[] { id, value });
	}
	
	
	private final static StructType STRING_SCHEMA = createPrimitiveRowStruct(DataTypes.StringType);
	
	public final static Row STRING_1_1_JUDE = RowFactory.create("abc1", "Jude");
	public final static Row STRING_2_1_SAMUEL = RowFactory.create("abc2", "Samuel");
	public final static Row STRING_2_2_TOMAS = RowFactory.create("abc2", "Thomas");
	public final static Row STRING_3_1_NULL = RowFactory.create("abc3", null);
	public final static Row STRING_4_1_ADAM = RowFactory.create("abc4", "Adam");
	public final static Row STRING_4_2_ADAM = RowFactory.create("abc4", "Adam");
	public final static Row STRING_5_1_NULL = RowFactory.create("abc5", null);
	public final static Row STRING_5_2_NULL = RowFactory.create("abc5", null);
	
	
	public final static List<Row> STRING_ALL_ROWS = Arrays.asList(STRING_1_1_JUDE, STRING_2_1_SAMUEL, STRING_2_2_TOMAS, STRING_3_1_NULL,
			STRING_4_1_ADAM, STRING_4_2_ADAM, STRING_5_1_NULL, STRING_5_2_NULL);

	public final static List<Row> NO_ROWS = Collections.emptyList();
		
  public static Dataset<Row> createCompleteDataset(SparkSession spark) {
  	return spark.createDataFrame(STRING_ALL_ROWS, STRING_SCHEMA);
  }
  
  public static Dataset<Row> createEmptyDataset(SparkSession spark) {
  	return spark.createDataFrame(NO_ROWS, STRING_SCHEMA);
  }
	
}
