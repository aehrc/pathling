/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.sql.udf;

import org.apache.spark.sql.types.DataType;

/**
 * The interface that encapsulates meta data required for registration of a custom UDF function in
 * Spark.
 */
public interface SqlFunction {

  /**
   * Gets the name of the UDF.
   *
   * @return the name of the UDF function.
   */
  String getName();

  /**
   * Gets the return type of the UDF
   *
   * @return the SQL type returned by the UDF.
   */
  DataType getReturnType();

}
