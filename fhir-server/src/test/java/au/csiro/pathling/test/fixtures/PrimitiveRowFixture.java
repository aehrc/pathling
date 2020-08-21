/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test.fixtures;

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

  public static StructType createPrimitiveRowStruct(final DataType dataType) {
    final Metadata metadata = new MetadataBuilder().build();
    final StructField id = new StructField("123abcd_id", DataTypes.StringType, false, metadata);
    final StructField value = new StructField("123abcd", dataType, true, metadata);
    return new StructType(new StructField[]{id, value});
  }

}
