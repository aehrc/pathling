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
