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

package au.csiro.pathling.utilities;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Test;

class SparkTypesTest {

  @Test
  void returnsTheElementTypeOfAnArray() {
    assertEquals(
        DataTypes.StringType,
        SparkTypes.elementTypeOf(DataTypes.createArrayType(DataTypes.StringType)));
  }

  @Test
  void returnsATypeThatIsNotAnArrayAsItIs() {
    assertEquals(DataTypes.IntegerType, SparkTypes.elementTypeOf(DataTypes.IntegerType));
  }

  @Test
  void unwrapsOnlyOneLevelOfArray() {
    final DataType inner = DataTypes.createArrayType(DataTypes.StringType);
    assertEquals(inner, SparkTypes.elementTypeOf(DataTypes.createArrayType(inner)));
  }
}
