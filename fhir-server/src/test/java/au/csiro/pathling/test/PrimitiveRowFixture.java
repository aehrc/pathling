/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.test;

import org.apache.spark.sql.types.*;

/**
 * @author John Grimes
 */
public class PrimitiveRowFixture {

  public final static String ROW_ID_1 = "abc1";
  public final static String ROW_ID_2 = "abc2";
  public final static String ROW_ID_3 = "abc3";
  public final static String ROW_ID_4 = "abc4";
  public final static String ROW_ID_5 = "abc5";

  public static StructType createPrimitiveRowStruct(DataType dataType) {
    Metadata metadata = new MetadataBuilder().build();
    StructField id = new StructField("123abcd_id", DataTypes.StringType, false, metadata);
    StructField value = new StructField("123abcd", dataType, true, metadata);
    return new StructType(new StructField[] {id, value});
  }

}
