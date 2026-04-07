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

package au.csiro.pathling.fhirpath.evaluation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.fhirpath.ListTraceCollector;
import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.DecimalCollection;
import au.csiro.pathling.fhirpath.collection.IntegerCollection;
import au.csiro.pathling.fhirpath.collection.StringCollection;
import au.csiro.pathling.fhirpath.evaluation.SingleInstanceEvaluationResult.TraceResult;
import au.csiro.pathling.fhirpath.evaluation.SingleInstanceEvaluationResult.TypedValue;
import au.csiro.pathling.test.helpers.SqlHelpers;
import java.math.BigDecimal;
import java.util.List;
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
    void stripsNullNestedStructValues() {
      // A null nested struct value should be stripped as a null-valued field.
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

      assertEquals(1, sanitised.schema().fields().length);
      assertEquals("display", sanitised.schema().fields()[0].name());
      assertEquals("outer_val", sanitised.get(0));
    }

    @Test
    void stripsNullValuedFields() {
      // Fields with null values should be removed from the row.
      final StructType schema =
          new StructType(
              new StructField[] {
                DataTypes.createStructField("id", DataTypes.StringType, true),
                DataTypes.createStructField("value", DataTypes.StringType, true),
                DataTypes.createStructField("comparator", DataTypes.StringType, true),
                DataTypes.createStructField("unit", DataTypes.StringType, true),
              });

      final Row row = new GenericRowWithSchema(new Object[] {null, "100", null, "mg"}, schema);

      final Row sanitised = SingleInstanceEvaluator.sanitiseRow(row);

      assertEquals(2, sanitised.schema().fields().length);
      assertEquals("value", sanitised.schema().fields()[0].name());
      assertEquals("unit", sanitised.schema().fields()[1].name());
      assertEquals("100", sanitised.get(0));
      assertEquals("mg", sanitised.get(1));
    }

    @Test
    void stripsNullValuedFieldsFromNestedStructs() {
      // Null-valued fields in nested struct types should also be stripped.
      final StructType nestedSchema =
          new StructType(
              new StructField[] {
                DataTypes.createStructField("start", DataTypes.StringType, true),
                DataTypes.createStructField("end", DataTypes.StringType, true),
              });

      final StructType outerSchema =
          new StructType(
              new StructField[] {
                DataTypes.createStructField("family", DataTypes.StringType, true),
                DataTypes.createStructField("text", DataTypes.StringType, true),
                DataTypes.createStructField("period", nestedSchema, true),
              });

      final Row nestedRow =
          new GenericRowWithSchema(new Object[] {"2020-01-01", null}, nestedSchema);
      final Row outerRow =
          new GenericRowWithSchema(new Object[] {"Smith", null, nestedRow}, outerSchema);

      final Row sanitised = SingleInstanceEvaluator.sanitiseRow(outerRow);

      // The outer null-valued "text" should be removed.
      assertEquals(2, sanitised.schema().fields().length);
      assertEquals("family", sanitised.schema().fields()[0].name());
      assertEquals("period", sanitised.schema().fields()[1].name());

      // The nested null-valued "end" should also be removed.
      final Row sanitisedNested = sanitised.getAs("period");
      assertNotNull(sanitisedNested);
      assertEquals(1, sanitisedNested.schema().fields().length);
      assertEquals("start", sanitisedNested.schema().fields()[0].name());
      assertEquals("2020-01-01", sanitisedNested.get(0));
    }

    @Test
    void updatesParentSchemaForSanitisedNestedStructs() {
      // The parent's StructField dataType for a nested struct must match the sanitised nested
      // Row's schema, so that Row.json() positional mapping remains correct.
      final StructType nestedSchema =
          new StructType(
              new StructField[] {
                DataTypes.createStructField("id", DataTypes.StringType, true),
                DataTypes.createStructField("start", DataTypes.StringType, true),
                DataTypes.createStructField("end", DataTypes.StringType, true),
              });

      final StructType outerSchema =
          new StructType(
              new StructField[] {
                DataTypes.createStructField("family", DataTypes.StringType, true),
                DataTypes.createStructField("period", nestedSchema, true),
              });

      final Row nestedRow =
          new GenericRowWithSchema(new Object[] {null, "2000", "2002"}, nestedSchema);
      final Row outerRow = new GenericRowWithSchema(new Object[] {"Smith", nestedRow}, outerSchema);

      final Row sanitised = SingleInstanceEvaluator.sanitiseRow(outerRow);

      // The parent's "period" field dataType should have 2 fields (id stripped as null).
      final StructType periodType = (StructType) sanitised.schema().apply("period").dataType();
      assertEquals(2, periodType.fields().length);
      assertEquals("start", periodType.fields()[0].name());
      assertEquals("end", periodType.fields()[1].name());

      // The parent's dataType should match the nested Row's own schema.
      final Row sanitisedNested = sanitised.getAs("period");
      assertEquals(sanitisedNested.schema(), periodType);
    }

    @Test
    void preservesFieldsWithNonNullValues() {
      // All non-null fields should be kept, even if some are null.
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
              new Object[] {"1.5", "mmol/L", "http://unitsofmeasure.org", "mmol/L"}, schema);

      final Row sanitised = SingleInstanceEvaluator.sanitiseRow(row);

      assertEquals(4, sanitised.schema().fields().length);
      assertEquals("1.5", sanitised.get(0));
      assertEquals("mmol/L", sanitised.get(1));
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

    @Test
    void jsonCorrectlyMapsNestedStructFieldsAfterNullStripping() {
      // Nested struct with null fields should produce correct field-to-value mapping in JSON.
      // Without the fix, Row.json() uses the parent's original dataType to interpret the nested
      // row, causing positional misalignment when null fields have been stripped.
      final StructType nestedSchema =
          new StructType(
              new StructField[] {
                DataTypes.createStructField("id", DataTypes.StringType, true),
                DataTypes.createStructField("start", DataTypes.StringType, true),
                DataTypes.createStructField("end", DataTypes.StringType, true),
              });

      final StructType outerSchema =
          new StructType(
              new StructField[] {
                DataTypes.createStructField("family", DataTypes.StringType, true),
                DataTypes.createStructField("period", nestedSchema, true),
              });

      final Row nestedRow =
          new GenericRowWithSchema(new Object[] {null, "2000", "2002"}, nestedSchema);
      final Row outerRow = new GenericRowWithSchema(new Object[] {"Smith", nestedRow}, outerSchema);

      final String json = SingleInstanceEvaluator.rowToJson(outerRow);

      assertTrue(json.contains("\"start\":\"2000\""));
      assertTrue(json.contains("\"end\":\"2002\""));
      assertFalse(json.contains("\"id\""));
      assertTrue(json.contains("\"family\":\"Smith\""));
    }

    @Test
    void jsonExcludesNullValuedFields() {
      // The JSON output should not contain fields with null values.
      final StructType schema =
          new StructType(
              new StructField[] {
                DataTypes.createStructField("id", DataTypes.StringType, true),
                DataTypes.createStructField("value", DataTypes.StringType, true),
                DataTypes.createStructField("comparator", DataTypes.StringType, true),
                DataTypes.createStructField("unit", DataTypes.StringType, true),
              });

      final Row row = new GenericRowWithSchema(new Object[] {null, "100", null, "mg"}, schema);

      final String json = SingleInstanceEvaluator.rowToJson(row);

      assertFalse(json.contains("\"id\""));
      assertFalse(json.contains("\"comparator\""));
      assertTrue(json.contains("\"value\":\"100\""));
      assertTrue(json.contains("\"unit\":\"mg\""));
    }
  }

  @Nested
  class BuildTraceResultsTests {

    @Test
    void emptyCollectorReturnsEmptyList() {
      // An empty collector should produce an empty trace result list.
      final ListTraceCollector collector = new ListTraceCollector();
      final List<TraceResult> results = SingleInstanceEvaluator.buildTraceResults(collector);
      assertTrue(results.isEmpty());
    }

    @Test
    void singlePrimitiveEntry() {
      // A single primitive entry should produce one TraceResult with one TypedValue.
      final ListTraceCollector collector = new ListTraceCollector();
      collector.add("lbl", "string", "hello");

      final List<TraceResult> results = SingleInstanceEvaluator.buildTraceResults(collector);

      assertEquals(1, results.size());
      assertEquals("lbl", results.get(0).getLabel());
      assertEquals("string", results.get(0).getFhirType());
      assertEquals(1, results.get(0).getValues().size());
      assertEquals("string", results.get(0).getValues().get(0).getType());
      assertEquals("hello", results.get(0).getValues().get(0).getValue());
    }

    @Test
    void singleNullValueEntry() {
      // A null value should produce a TypedValue with null.
      final ListTraceCollector collector = new ListTraceCollector();
      collector.add("lbl", "string", null);

      final List<TraceResult> results = SingleInstanceEvaluator.buildTraceResults(collector);

      assertEquals(1, results.size());
      assertEquals(1, results.get(0).getValues().size());
      assertNull(results.get(0).getValues().get(0).getValue());
    }

    @Test
    void multipleEntriesSameLabelGrouped() {
      // Entries with the same label should be grouped into one TraceResult.
      final ListTraceCollector collector = new ListTraceCollector();
      collector.add("x", "string", "a");
      collector.add("x", "string", "b");

      final List<TraceResult> results = SingleInstanceEvaluator.buildTraceResults(collector);

      assertEquals(1, results.size());
      assertEquals("x", results.get(0).getLabel());
      assertEquals(2, results.get(0).getValues().size());
      assertEquals("a", results.get(0).getValues().get(0).getValue());
      assertEquals("b", results.get(0).getValues().get(1).getValue());
    }

    @Test
    void differentLabelsProduceSeparateResults() {
      // Entries with different labels should produce separate TraceResults.
      final ListTraceCollector collector = new ListTraceCollector();
      collector.add("a", "string", "v1");
      collector.add("b", "boolean", true);

      final List<TraceResult> results = SingleInstanceEvaluator.buildTraceResults(collector);

      assertEquals(2, results.size());
      assertEquals("a", results.get(0).getLabel());
      assertEquals("b", results.get(1).getLabel());
    }

    @Test
    void insertionOrderPreserved() {
      // Results should appear in the order labels were first seen, not alphabetically.
      final ListTraceCollector collector = new ListTraceCollector();
      collector.add("c", "string", "1");
      collector.add("a", "string", "2");
      collector.add("b", "string", "3");

      final List<TraceResult> results = SingleInstanceEvaluator.buildTraceResults(collector);

      assertEquals(3, results.size());
      assertEquals("c", results.get(0).getLabel());
      assertEquals("a", results.get(1).getLabel());
      assertEquals("b", results.get(2).getLabel());
    }

    @Test
    void rowValueConvertedToJson() {
      // A Row value should be converted to a JSON string.
      final StructType schema =
          new StructType(
              new StructField[] {
                DataTypes.createStructField("value", DataTypes.StringType, true),
                DataTypes.createStructField("code", DataTypes.StringType, true),
              });
      final Row row = new GenericRowWithSchema(new Object[] {"100", "mg"}, schema);

      final ListTraceCollector collector = new ListTraceCollector();
      collector.add("qty", "Quantity", row);

      final List<TraceResult> results = SingleInstanceEvaluator.buildTraceResults(collector);

      assertEquals(1, results.size());
      final Object value = results.get(0).getValues().get(0).getValue();
      assertInstanceOf(String.class, value);
      final String json = (String) value;
      assertTrue(json.contains("\"value\":\"100\""));
      assertTrue(json.contains("\"code\":\"mg\""));
    }

    @Test
    void rowValueWithSyntheticFieldsStripped() {
      // Synthetic fields in Row values should be stripped in the JSON output.
      final StructType schema =
          new StructType(
              new StructField[] {
                DataTypes.createStructField("value", DataTypes.StringType, true),
                DataTypes.createStructField("_fid", DataTypes.IntegerType, true),
                DataTypes.createStructField("value_scale", DataTypes.IntegerType, true),
              });
      final Row row = new GenericRowWithSchema(new Object[] {"100", 1, 3}, schema);

      final ListTraceCollector collector = new ListTraceCollector();
      collector.add("qty", "Quantity", row);

      final List<TraceResult> results = SingleInstanceEvaluator.buildTraceResults(collector);

      final String json = (String) results.get(0).getValues().get(0).getValue();
      assertFalse(json.contains("_fid"));
      assertFalse(json.contains("value_scale"));
      assertTrue(json.contains("\"value\":\"100\""));
    }

    @Test
    void scalaSeqValueExpanded() {
      // A Scala Seq value should be expanded into individual TypedValues.
      final ListTraceCollector collector = new ListTraceCollector();
      collector.add("items", "string", SqlHelpers.sql_array("a", "b", "c"));

      final List<TraceResult> results = SingleInstanceEvaluator.buildTraceResults(collector);

      assertEquals(1, results.size());
      final List<TypedValue> values = results.get(0).getValues();
      assertEquals(3, values.size());
      assertEquals("a", values.get(0).getValue());
      assertEquals("b", values.get(1).getValue());
      assertEquals("c", values.get(2).getValue());
    }

    @Test
    void scalaSeqWithRowElementsConvertedToJson() {
      // A Scala Seq of Rows should produce JSON strings for each element.
      final StructType schema =
          new StructType(
              new StructField[] {
                DataTypes.createStructField("code", DataTypes.StringType, true),
              });
      final Row row1 = new GenericRowWithSchema(new Object[] {"mg"}, schema);
      final Row row2 = new GenericRowWithSchema(new Object[] {"kg"}, schema);

      final ListTraceCollector collector = new ListTraceCollector();
      collector.add("codes", "Coding", SqlHelpers.sql_array(row1, row2));

      final List<TraceResult> results = SingleInstanceEvaluator.buildTraceResults(collector);

      final List<TypedValue> values = results.get(0).getValues();
      assertEquals(2, values.size());
      assertInstanceOf(String.class, values.get(0).getValue());
      assertTrue(((String) values.get(0).getValue()).contains("\"code\":\"mg\""));
      assertTrue(((String) values.get(1).getValue()).contains("\"code\":\"kg\""));
    }

    @Test
    void mixedLabelsAndTypes() {
      // Multiple labels with different types should be grouped correctly.
      final ListTraceCollector collector = new ListTraceCollector();
      collector.add("name", "HumanName", "Smith");
      collector.add("active", "boolean", true);
      collector.add("name", "HumanName", "Jones");

      final List<TraceResult> results = SingleInstanceEvaluator.buildTraceResults(collector);

      assertEquals(2, results.size());
      assertEquals("name", results.get(0).getLabel());
      assertEquals(2, results.get(0).getValues().size());
      assertEquals("active", results.get(1).getLabel());
      assertEquals(1, results.get(1).getValues().size());
    }

    @Test
    void fhirTypeFromFirstEntryInGroup() {
      // The fhirType on TraceResult should come from the first entry in the group.
      final ListTraceCollector collector = new ListTraceCollector();
      collector.add("val", "string", "hello");
      collector.add("val", "string", "world");

      final List<TraceResult> results = SingleInstanceEvaluator.buildTraceResults(collector);

      assertEquals("string", results.get(0).getFhirType());
    }
  }
}
