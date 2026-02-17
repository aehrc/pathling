/*
 * Copyright 2018-2026 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.fhirpath.evaluation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.DecimalCollection;
import au.csiro.pathling.fhirpath.collection.IntegerCollection;
import au.csiro.pathling.fhirpath.collection.StringCollection;
import java.math.BigDecimal;
import java.util.Map;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link SingleInstanceEvaluator} utility methods: variable conversion and row
 * sanitisation.
 *
 * @author John Grimes
 */
class SingleInstanceEvaluatorTest {

  @Nested
  class ConvertVariablesTests {

    @Test
    void convertsStringVariable() {
      // A String value should produce a StringCollection.
      final Map<String, Collection> result =
          SingleInstanceEvaluator.convertVariables(Map.of("greeting", "hello"));

      assertEquals(1, result.size());
      assertInstanceOf(StringCollection.class, result.get("greeting"));
    }

    @Test
    void convertsIntegerVariable() {
      // An Integer value should produce an IntegerCollection.
      final Map<String, Collection> result =
          SingleInstanceEvaluator.convertVariables(Map.of("count", 42));

      assertEquals(1, result.size());
      assertInstanceOf(IntegerCollection.class, result.get("count"));
    }

    @Test
    void convertsBooleanVariable() {
      // A Boolean value should produce a BooleanCollection.
      final Map<String, Collection> result =
          SingleInstanceEvaluator.convertVariables(Map.of("flag", true));

      assertEquals(1, result.size());
      assertInstanceOf(BooleanCollection.class, result.get("flag"));
    }

    @Test
    void convertsDecimalVariable() {
      // A BigDecimal value should produce a DecimalCollection.
      final Map<String, Collection> result =
          SingleInstanceEvaluator.convertVariables(Map.of("rate", new BigDecimal("3.14")));

      assertEquals(1, result.size());
      assertInstanceOf(DecimalCollection.class, result.get("rate"));
    }

    @Test
    void convertsDoubleVariable() {
      // A Double value (as sent by py4j) should produce a DecimalCollection.
      final Map<String, Collection> result =
          SingleInstanceEvaluator.convertVariables(Map.of("rate", 3.14));

      assertEquals(1, result.size());
      assertInstanceOf(DecimalCollection.class, result.get("rate"));
    }

    @Test
    void returnsEmptyMapForNull() {
      // A null variables map should produce an empty map.
      final Map<String, Collection> result = SingleInstanceEvaluator.convertVariables(null);
      assertTrue(result.isEmpty());
    }

    @Test
    void returnsEmptyMapForEmptyInput() {
      // An empty variables map should produce an empty map.
      final Map<String, Collection> result = SingleInstanceEvaluator.convertVariables(Map.of());
      assertTrue(result.isEmpty());
    }

    @Test
    void convertsMultipleVariables() {
      // Multiple variables of different types should all be converted.
      final Map<String, Object> input = Map.of("name", "Alice", "age", 30, "active", true);

      final Map<String, Collection> result = SingleInstanceEvaluator.convertVariables(input);

      assertEquals(3, result.size());
      assertInstanceOf(StringCollection.class, result.get("name"));
      assertInstanceOf(IntegerCollection.class, result.get("age"));
      assertInstanceOf(BooleanCollection.class, result.get("active"));
    }

    @Test
    void throwsForUnsupportedType() {
      // An unsupported type should throw IllegalArgumentException.
      final Map<String, Object> input = Map.of("bad", new Object());
      assertThrows(
          IllegalArgumentException.class, () -> SingleInstanceEvaluator.convertVariables(input));
    }
  }

  @Nested
  class SanitiseRowTests {

    @Test
    void stripsSyntheticFieldsFromRow() {
      // A row with synthetic fields (_fid, value_scale) should have them removed.
      final StructType schema =
          new StructType(
              new StructField[] {
                DataTypes.createStructField("value", DataTypes.StringType, true),
                DataTypes.createStructField("_fid", DataTypes.IntegerType, true),
                DataTypes.createStructField("code", DataTypes.StringType, true),
                DataTypes.createStructField("value_scale", DataTypes.IntegerType, true),
              });

      final Row row = new GenericRowWithSchema(new Object[] {"100", 1, "mg", 3}, schema);

      final Row sanitised = SingleInstanceEvaluator.sanitiseRow(row);

      assertEquals(2, sanitised.schema().fields().length);
      assertEquals("value", sanitised.schema().fields()[0].name());
      assertEquals("code", sanitised.schema().fields()[1].name());
      assertEquals("100", sanitised.get(0));
      assertEquals("mg", sanitised.get(1));
    }

    @Test
    void stripsCanonicalizedFields() {
      // Fields like _value_canonicalized and _code_canonicalized should be stripped.
      final StructType schema =
          new StructType(
              new StructField[] {
                DataTypes.createStructField("value", DataTypes.StringType, true),
                DataTypes.createStructField("_value_canonicalized", DataTypes.StringType, true),
                DataTypes.createStructField("code", DataTypes.StringType, true),
                DataTypes.createStructField("_code_canonicalized", DataTypes.StringType, true),
              });

      final Row row = new GenericRowWithSchema(new Object[] {"100", "100.0", "mg", "mg"}, schema);

      final Row sanitised = SingleInstanceEvaluator.sanitiseRow(row);

      assertEquals(2, sanitised.schema().fields().length);
      assertEquals("value", sanitised.schema().fields()[0].name());
      assertEquals("code", sanitised.schema().fields()[1].name());
    }

    @Test
    void preservesNonSyntheticFields() {
      // A row with no synthetic fields should remain unchanged.
      final StructType schema =
          new StructType(
              new StructField[] {
                DataTypes.createStructField("value", DataTypes.StringType, true),
                DataTypes.createStructField("unit", DataTypes.StringType, true),
                DataTypes.createStructField("system", DataTypes.StringType, true),
                DataTypes.createStructField("code", DataTypes.StringType, true),
              });

      final Row row =
          new GenericRowWithSchema(
              new Object[] {"100", "mg", "http://unitsofmeasure.org", "mg"}, schema);

      final Row sanitised = SingleInstanceEvaluator.sanitiseRow(row);

      assertEquals(4, sanitised.schema().fields().length);
      assertEquals("100", sanitised.get(0));
      assertEquals("mg", sanitised.get(1));
    }

    @Test
    void sanitisesNestedStructs() {
      // Synthetic fields in nested struct types should also be stripped.
      final StructType nestedSchema =
          new StructType(
              new StructField[] {
                DataTypes.createStructField("value", DataTypes.StringType, true),
                DataTypes.createStructField("_fid", DataTypes.IntegerType, true),
              });

      final StructType outerSchema =
          new StructType(
              new StructField[] {
                DataTypes.createStructField("display", DataTypes.StringType, true),
                DataTypes.createStructField("nested", nestedSchema, true),
                DataTypes.createStructField("_fid", DataTypes.IntegerType, true),
              });

      final Row nestedRow = new GenericRowWithSchema(new Object[] {"inner_val", 99}, nestedSchema);
      final Row outerRow =
          new GenericRowWithSchema(new Object[] {"outer_val", nestedRow, 1}, outerSchema);

      final Row sanitised = SingleInstanceEvaluator.sanitiseRow(outerRow);

      // The outer _fid should be removed.
      assertEquals(2, sanitised.schema().fields().length);
      assertEquals("display", sanitised.schema().fields()[0].name());
      assertEquals("nested", sanitised.schema().fields()[1].name());

      // The nested _fid should also be removed.
      final Row sanitisedNested = sanitised.getAs("nested");
      assertNotNull(sanitisedNested);
      assertEquals(1, sanitisedNested.schema().fields().length);
      assertEquals("value", sanitisedNested.schema().fields()[0].name());
      assertEquals("inner_val", sanitisedNested.get(0));
    }

    @Test
    void handlesNullNestedValues() {
      // A null nested struct value should be preserved without error.
      final StructType nestedSchema =
          new StructType(
              new StructField[] {
                DataTypes.createStructField("value", DataTypes.StringType, true),
                DataTypes.createStructField("_fid", DataTypes.IntegerType, true),
              });

      final StructType outerSchema =
          new StructType(
              new StructField[] {
                DataTypes.createStructField("display", DataTypes.StringType, true),
                DataTypes.createStructField("nested", nestedSchema, true),
              });

      final Row outerRow = new GenericRowWithSchema(new Object[] {"outer_val", null}, outerSchema);

      final Row sanitised = SingleInstanceEvaluator.sanitiseRow(outerRow);

      assertEquals(2, sanitised.schema().fields().length);
      assertEquals("outer_val", sanitised.get(0));
      assertNull(sanitised.get(1));
    }
  }

  @Nested
  class RowToJsonTests {

    @Test
    void jsonExcludesSyntheticFields() {
      // The JSON output should not contain any synthetic fields.
      final StructType schema =
          new StructType(
              new StructField[] {
                DataTypes.createStructField("value", DataTypes.StringType, true),
                DataTypes.createStructField("_fid", DataTypes.IntegerType, true),
                DataTypes.createStructField("code", DataTypes.StringType, true),
                DataTypes.createStructField("value_scale", DataTypes.IntegerType, true),
              });

      final Row row = new GenericRowWithSchema(new Object[] {"100", 1, "mg", 3}, schema);

      final String json = SingleInstanceEvaluator.rowToJson(row);

      assertFalse(json.contains("_fid"));
      assertFalse(json.contains("value_scale"));
      assertTrue(json.contains("\"value\":\"100\""));
      assertTrue(json.contains("\"code\":\"mg\""));
    }
  }
}
