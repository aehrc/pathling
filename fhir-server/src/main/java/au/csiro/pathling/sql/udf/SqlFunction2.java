/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.sql.udf;

import org.apache.spark.sql.api.java.UDF2;

/**
 * A registrable UDF function with two arguments.
 *
 * @param <T1> the type of the first argument.
 * @param <T2> the type of the second argument.
 * @param <R> the type of the result.
 */
public interface SqlFunction2<T1, T2, R> extends SqlFunction, UDF2<T1, T2, R> {

}
