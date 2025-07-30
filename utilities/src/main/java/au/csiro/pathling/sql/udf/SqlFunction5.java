/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.sql.udf;

import org.apache.spark.sql.api.java.UDF5;

/**
 * A registrable UDF function with three arguments.
 *
 * @param <T1> the type of the first argument.
 * @param <T2> the type of the second argument.
 * @param <T3> the type of the third argument.
 * @param <T4> the type of the fourth argument.
 * @param <T5> the type of the fifth argument.
 * @param <R> the type of the result.
 */
public interface SqlFunction5<T1, T2, T3, T4, T5, R> extends SqlFunction,
    UDF5<T1, T2, T3, T4, T5, R> {

}
