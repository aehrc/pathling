/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.csiro.pathling.sql.udf;

import org.apache.spark.sql.api.java.UDF3;

/**
 * A registrable UDF function with three arguments.
 *
 * @param <T1> the type of the first argument.
 * @param <T2> the type of the second argument.
 * @param <T3> the type of the third argument.
 * @param <R> the type of the result.
 */
public interface SqlFunction3<T1, T2, T3, R> extends SqlFunction, UDF3<T1, T2, T3, R> {}
