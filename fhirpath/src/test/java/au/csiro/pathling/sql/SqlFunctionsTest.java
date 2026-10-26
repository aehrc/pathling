/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import au.csiro.pathling.test.SpringBootUnitTest;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

@SpringBootUnitTest
class SqlFunctionsTest {

  @Autowired private SparkSession spark;

  @Test
  void testToFhirInstant() {
    final Dataset<Row> result =
        spark
            .range(1)
            .toDF()
            .select(
                SqlFunctions.toFhirInstant(
                        functions.lit("2023-01-01T12:34:56.789Z").cast(DataTypes.TimestampType))
                    .as("instant_utc"),
                SqlFunctions.toFhirInstant(
                        functions.lit("2023-01-01T12:34:56.7+10:00").cast(DataTypes.TimestampType))
                    .as("instant_plus_10"));
    final Row resultRow = result.first();

    assertEquals("2023-01-01T12:34:56.789Z", resultRow.getString(0));
    assertEquals("2023-01-01T02:34:56.700Z", resultRow.getString(1));
  }

  @Test
  void testToFhirInstantNull() {
    // Dataset with null timestamp
    final Dataset<Row> result =
        spark.range(1).toDF().select(SqlFunctions.toFhirInstant(functions.lit(null)).as("instant"));
    assertNull(result.first().get(0));
  }
}
