/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test.fixtures;

import au.csiro.pathling.test.builders.DatasetBuilder;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

@SuppressWarnings({"WeakerAccess", "unused"})
public class StringPrimitiveRowFixture extends PrimitiveRowFixture {

  private final static StructType STRING_SCHEMA =
      PrimitiveRowFixture.createPrimitiveRowStruct(DataTypes.StringType);

  public final static Row STRING_1_JUDE = RowFactory.create(ROW_ID_1, "Jude");
  public final static Row STRING_2_SAMUEL = RowFactory.create(ROW_ID_2, "Samuel");
  public final static Row STRING_2_THOMAS = RowFactory.create(ROW_ID_2, "Thomas");
  public final static Row STRING_3_NULL = RowFactory.create(ROW_ID_3, null);
  public final static Row STRING_4_NULL = RowFactory.create(ROW_ID_4, null);
  public final static Row STRING_4_ADAM = RowFactory.create(ROW_ID_4, "Adam");
  public final static Row STRING_5_NULL = RowFactory.create(ROW_ID_5, null);

  public final static List<String> STRING_ALL_IDS =
      Arrays.asList(ROW_ID_1, ROW_ID_2, ROW_ID_3, ROW_ID_4, ROW_ID_5);

  public final static List<Row> STRING_ALL_ROWS = Arrays.asList(STRING_1_JUDE, STRING_2_SAMUEL,
      STRING_2_THOMAS, STRING_3_NULL, STRING_4_NULL, STRING_4_ADAM, STRING_4_ADAM, STRING_5_NULL,
      STRING_5_NULL);

  public final static List<Row> STRING_NULL_ROWS =
      Arrays.asList(STRING_3_NULL, STRING_5_NULL, STRING_5_NULL);

  public final static List<Row> NO_ROWS = Collections.emptyList();

  public final static List<Row> STRING_ALL_ROWS_NULL =
      STRING_ALL_IDS.stream().map(s -> RowFactory.create(s, null)).collect(Collectors.toList());

  public static Dataset<Row> createCompleteDataset(final SparkSession spark) {
    return spark.createDataFrame(STRING_ALL_ROWS, STRING_SCHEMA);
  }

  public static Dataset<Row> createDataset(final SparkSession spark, final Row... rows) {
    return spark.createDataFrame(Arrays.asList(rows), STRING_SCHEMA);
  }

  public static Dataset<Row> createNullRowsDataset(final SparkSession spark) {
    return spark.createDataFrame(STRING_NULL_ROWS, STRING_SCHEMA);
  }

  public static Dataset<Row> createAllRowsNullDataset(final SparkSession spark) {
    return spark.createDataFrame(STRING_ALL_ROWS_NULL, STRING_SCHEMA);
  }

  public static Dataset<Row> createEmptyDataset(final SparkSession spark) {
    return spark.createDataFrame(NO_ROWS, STRING_SCHEMA);
  }

  public static DatasetBuilder allStringsWithValue(final Object value) {
    return new DatasetBuilder()
        .withIdColumn()
        .withColumn(DataTypes.BooleanType)
        .withIdsAndValue(value, STRING_ALL_IDS);
  }
}
