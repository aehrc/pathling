/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test;

import au.csiro.pathling.test.fixtures.PrimitiveRowFixture;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

/**
 * @author Piotr Szul
 */

public class CodingRowFixture extends PrimitiveRowFixture {

  public static StructType createCodingStruct(Metadata metadata) {
    StructType codingStruct = new StructType(
        new StructField[]{new StructField("id", DataTypes.StringType, true, metadata),
            new StructField("system", DataTypes.StringType, true, metadata),
            new StructField("version", DataTypes.StringType, true, metadata),
            new StructField("code", DataTypes.StringType, true, metadata),
            new StructField("display", DataTypes.StringType, true, metadata),
            new StructField("userSelected", DataTypes.BooleanType, true, metadata)});
    return codingStruct;
  }

  public static StructType createCodingRowStruct() {
    Metadata metadata = new MetadataBuilder().build();
    StructType codingStruct = createCodingStruct(metadata);
    StructField id = new StructField("123abcd_id", DataTypes.StringType, false, metadata);
    StructField codingField = new StructField("123abcd", codingStruct, false, metadata);
    return new StructType(new StructField[]{id, codingField});
  }

  public static final StructType SCHEMA = createCodingRowStruct();

  public static final String SYSTEM_1 = "uri:SYSTEM_1";
  public static final String VERSION_1 = "version-1";
  public static final String CODE_1 = "CODE_1";
  public static final String SYSTEM_2 = "uri:SYSTEM_2";
  public static final String VERSION_2 = "version-2";
  public static final String CODE_2 = "CODE_2";


  public static final Row CODING_1_S1_C1 = RowFactory.create(ROW_ID_1,
      RowFactory.create("id1", SYSTEM_1, null, CODE_1, "Some code 1", false));

  public static final Row CODING_1_S2_C2_V1 = RowFactory.create(ROW_ID_2,
      RowFactory.create("id2", SYSTEM_2, VERSION_1, CODE_2, "Some code 2", true));

  public static final Row CODING_2_S2_C2 = RowFactory.create(ROW_ID_2,
      RowFactory.create("id3", SYSTEM_2, null, CODE_2, "Some code 3", true));

  public static final Row CODING_3_S2_C2_V2 = RowFactory.create(ROW_ID_3,
      RowFactory.create("id4", SYSTEM_2, VERSION_2, CODE_2, "Some code 3", true));

  public static final Row CODING_3_S1_C1_V2 = RowFactory.create(ROW_ID_3,
      RowFactory.create("id5", SYSTEM_1, VERSION_2, CODE_1, "Some code 5", true));

  public static final Row CODING_4_NULL = RowFactory.create(ROW_ID_4, null);

  public final static List<Row> CODINGS_ALL_ROWS = Arrays.asList(CODING_1_S1_C1, CODING_1_S2_C2_V1,
      CODING_2_S2_C2, CODING_3_S2_C2_V2, CODING_3_S1_C1_V2, CODING_4_NULL);

  public static Dataset<Row> createCompleteDataset(SparkSession spark) {
    return spark.createDataFrame(CODINGS_ALL_ROWS, SCHEMA);
  }
}
