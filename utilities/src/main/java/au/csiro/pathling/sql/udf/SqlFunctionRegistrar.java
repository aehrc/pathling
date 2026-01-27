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

import au.csiro.pathling.spark.SparkConfigurer;
import jakarta.annotation.Nonnull;
import java.util.List;
import org.apache.spark.sql.SparkSession;

/** A spark configurer that registers user defined functions in the sessions. */
public class SqlFunctionRegistrar implements SparkConfigurer {

  @Nonnull private final List<SqlFunction1<?, ?>> sqlFunction1;
  @Nonnull private final List<SqlFunction2<?, ?, ?>> sqlFunction2;
  @Nonnull private final List<SqlFunction3<?, ?, ?, ?>> sqlFunction3;
  @Nonnull private final List<SqlFunction4<?, ?, ?, ?, ?>> sqlFunction4;

  @Nonnull private final List<SqlFunction5<?, ?, ?, ?, ?, ?>> sqlFunction5;

  /**
   * Creates a new SqlFunctionRegistrar with the specified SQL functions.
   *
   * @param sqlFunction1 list of single-parameter SQL functions
   * @param sqlFunction2 list of two-parameter SQL functions
   * @param sqlFunction3 list of three-parameter SQL functions
   * @param sqlFunction4 list of four-parameter SQL functions
   * @param sqlFunction5 list of five-parameter SQL functions
   */
  public SqlFunctionRegistrar(
      @Nonnull final List<SqlFunction1<?, ?>> sqlFunction1,
      @Nonnull final List<SqlFunction2<?, ?, ?>> sqlFunction2,
      @Nonnull final List<SqlFunction3<?, ?, ?, ?>> sqlFunction3,
      @Nonnull final List<SqlFunction4<?, ?, ?, ?, ?>> sqlFunction4,
      @Nonnull final List<SqlFunction5<?, ?, ?, ?, ?, ?>> sqlFunction5) {
    this.sqlFunction1 = sqlFunction1;
    this.sqlFunction2 = sqlFunction2;
    this.sqlFunction3 = sqlFunction3;
    this.sqlFunction4 = sqlFunction4;
    this.sqlFunction5 = sqlFunction5;
  }

  @Override
  public void configure(@Nonnull final SparkSession spark) {
    for (final SqlFunction1<?, ?> function : sqlFunction1) {
      spark.udf().register(function.getName(), function, function.getReturnType());
    }
    for (final SqlFunction2<?, ?, ?> function : sqlFunction2) {
      spark.udf().register(function.getName(), function, function.getReturnType());
    }
    for (final SqlFunction3<?, ?, ?, ?> function : sqlFunction3) {
      spark.udf().register(function.getName(), function, function.getReturnType());
    }
    for (final SqlFunction4<?, ?, ?, ?, ?> function : sqlFunction4) {
      spark.udf().register(function.getName(), function, function.getReturnType());
    }
    for (final SqlFunction5<?, ?, ?, ?, ?, ?> function : sqlFunction5) {
      spark.udf().register(function.getName(), function, function.getReturnType());
    }
  }
}
