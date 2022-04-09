/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.sql.udf;

import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataType;

public interface SqlFunction1<T1, R> extends UDF1<T1, R> {

  String getName();

  DataType getReturnType();

}
