/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.sql.udf;

import org.apache.spark.sql.api.java.UDF1;

/**
 * A registrable UDF function with one argument.
 *
 * @param <T1> the type of the argument.
 * @param <R> the type of the result.
 */
public interface SqlFunction1<T1, R> extends SqlFunction, UDF1<T1, R> {

}
