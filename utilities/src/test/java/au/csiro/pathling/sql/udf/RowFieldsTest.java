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

package au.csiro.pathling.sql.udf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

class RowFieldsTest {

  private static final StructType SCHEMA =
      new StructType()
          .add("text", DataTypes.StringType)
          .add("number", DataTypes.createDecimalType(10, 2));

  @Test
  void readsText() {
    final Row row = new GenericRowWithSchema(new Object[] {"abc", null}, SCHEMA);
    assertEquals("abc", RowFields.requireString(row, "text"));
  }

  @Test
  void readsNullText() {
    final Row row = new GenericRowWithSchema(new Object[] {null, null}, SCHEMA);
    assertNull(RowFields.requireString(row, "text"));
  }

  @Test
  void readsAbsentFieldAsNull() {
    final Row row = new GenericRowWithSchema(new Object[] {"abc", null}, SCHEMA);
    assertNull(RowFields.requireString(row, "missing"));
  }

  @Test
  void rejectsFieldOfAnotherType() {
    final Row row = new GenericRowWithSchema(new Object[] {null, new BigDecimal("1.50")}, SCHEMA);
    final IllegalArgumentException error =
        assertThrows(IllegalArgumentException.class, () -> RowFields.requireString(row, "number"));
    assertEquals("Field 'number' must be a string, but is decimal(10,2)", error.getMessage());
  }

  @Test
  void rejectsFieldOfAnotherTypeEvenWhenNull() {
    final Row row = new GenericRowWithSchema(new Object[] {null, null}, SCHEMA);
    assertThrows(IllegalArgumentException.class, () -> RowFields.requireString(row, "number"));
  }
}
