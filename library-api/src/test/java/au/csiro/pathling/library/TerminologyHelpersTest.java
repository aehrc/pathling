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

package au.csiro.pathling.library;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class TerminologyHelpersTest {

  private static SparkSession spark;

  @BeforeAll
  static void setUp() {
    spark = TestHelpers.spark();
  }

  @Test
  void toCoding() {
    final List<Row> rows = List.of(RowFactory.create("36470004"));
    final StructField codeField =
        new StructField("code", DataTypes.StringType, true, new MetadataBuilder().build());
    final Dataset<Row> dataset =
        spark.createDataFrame(
            rows, new StructType(List.of(codeField).toArray(new StructField[] {})));

    final Column codeColumn = dataset.col("code");
    final Column codingColumn =
        TerminologyHelpers.toCoding(codeColumn, "http://snomed.info/sct", null);

    final Dataset<Row> result = dataset.select(codingColumn);
    final List<Row> resultRows = result.collectAsList();
    final Row codingRow = resultRows.get(0).getStruct(0);

    assertNull(codingRow.get(0));
    assertEquals("http://snomed.info/sct", codingRow.get(1));
    assertNull(codingRow.get(2));
    assertEquals("36470004", codingRow.getString(3));
    assertNull(codingRow.get(4));
    assertNull(codingRow.get(5));
  }

  @Test
  void toEclValueSet() {
    final String ecl =
        "( (^929360071000036103|Medicinal product unit of use refset| :   {  "
            + " 700000111000036105|Strength reference set| >= #10000,   177631000036102|has unit| ="
            + " 700000881000036108|microgram/each|,   700000081000036101|has intended active"
            + " ingredient| = 1978011000036103|codeine|  },  {   700000111000036105|Strength"
            + " reference set| >= #250,   177631000036102|has unit| = 700000801000036102|mg/each|, "
            + "  700000081000036101|has intended active ingredient| = 2442011000036104|paracetamol|"
            + "  },  30523011000036108|has manufactured dose form| = 154011000036109|tablet| ))";
    final String expected =
        "http://snomed.info/sct?fhir_vs=ecl/%28%20%28%5E929360071000036103%7CMe"
            + "dicinal%20product%20unit%20of%20use%20refset%7C%20%3A%20%20%20%7B%20%20%2070000011100003"
            + "6105%7CStrength%20reference%20set%7C%20%3E%3D%20%2310000%2C%20%20%20177631000036102%7Cha"
            + "s%20unit%7C%20%3D%20700000881000036108%7Cmicrogram%2Feach%7C%2C%20%20%207000000810000361"
            + "01%7Chas%20intended%20active%20ingredient%7C%20%3D%201978011000036103%7Ccodeine%7C%20%20"
            + "%7D%2C%20%20%7B%20%20%20700000111000036105%7CStrength%20reference%20set%7C%20%3E%3D%20%2"
            + "3250%2C%20%20%20177631000036102%7Chas%20unit%7C%20%3D%20700000801000036102%7Cmg%2Feach%7"
            + "C%2C%20%20%20700000081000036101%7Chas%20intended%20active%20ingredient%7C%20%3D%20244201"
            + "1000036104%7Cparacetamol%7C%20%20%7D%2C%20%2030523011000036108%7Chas%20manufactured%20do"
            + "se%20form%7C%20%3D%20154011000036109%7Ctablet%7C%20%29%29";
    assertEquals(expected, TerminologyHelpers.toEclValueSet(ecl));
  }
}
