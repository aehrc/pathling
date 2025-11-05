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

package au.csiro.pathling.projection;

import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.createArrayType;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.apache.spark.sql.types.DataTypes.createStructType;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link SchemaUtils}.
 *
 * @author Piotr Szul
 */
class SchemaUtilsTest {

  @Test
  void testEmptySchema() {
    final StructType emptySchema = createStructType(List.of());
    assertEquals(0, SchemaUtils.computeMaxNestingLevel(emptySchema));
  }

  @Test
  void testFlatSchema() {
    // Schema: { id: String, name: String, age: Integer }
    final StructType flatSchema = createStructType(List.of(
        createStructField("id", StringType, true),
        createStructField("name", StringType, true),
        createStructField("age", IntegerType, true)
    ));

    // Each field appears once, so max nesting is 1
    assertEquals(1, SchemaUtils.computeMaxNestingLevel(flatSchema));
  }

  @Test
  void testNestedStructWithDifferentNames() {
    // Schema: { person: { name: String, address: { street: String } } }
    final StructType addressSchema = createStructType(List.of(
        createStructField("street", StringType, true)
    ));

    final StructType personSchema = createStructType(List.of(
        createStructField("name", StringType, true),
        createStructField("address", addressSchema, true)
    ));

    final StructType schema = createStructType(List.of(
        createStructField("person", personSchema, true)
    ));

    // Path: person -> name (person:1, name:1)
    // Path: person -> address -> street (person:1, address:1, street:1)
    // Max count is 1
    assertEquals(1, SchemaUtils.computeMaxNestingLevel(schema));
  }

  @Test
  void testRecursiveStructWithSameFieldName() {
    // Schema representing QuestionnaireResponse-like structure:
    // { item: Array<{ linkId: String, item: Array<{ linkId: String }> }> }
    final StructType innerItemSchema = createStructType(List.of(
        createStructField("linkId", StringType, true)
    ));

    final StructType outerItemSchema = createStructType(List.of(
        createStructField("linkId", StringType, true),
        createStructField("item", createArrayType(innerItemSchema), true)
    ));

    final StructType schema = createStructType(List.of(
        createStructField("item", createArrayType(outerItemSchema), true)
    ));

    // Path: item -> linkId (item:1, linkId:1)
    // Path: item -> item -> linkId (item:2, linkId:1)
    // Max count is 2 (for "item")
    assertEquals(2, SchemaUtils.computeMaxNestingLevel(schema));
  }

  @Test
  void testDeeplyNestedStructWithRepeatedFieldNames() {
    // Schema: { item: Array<{ item: Array<{ item: Array<{ linkId: String }> }> }> }
    final StructType level3Schema = createStructType(List.of(
        createStructField("linkId", StringType, true)
    ));

    final StructType level2Schema = createStructType(List.of(
        createStructField("item", createArrayType(level3Schema), true)
    ));

    final StructType level1Schema = createStructType(List.of(
        createStructField("item", createArrayType(level2Schema), true)
    ));

    final StructType schema = createStructType(List.of(
        createStructField("item", createArrayType(level1Schema), true)
    ));

    // Path: item -> item -> item -> linkId
    // Field "item" appears 3 times
    assertEquals(3, SchemaUtils.computeMaxNestingLevel(schema));
  }

  @Test
  void testMultipleBranchesWithDifferentNesting() {
    // Schema with two branches with different nesting levels:
    // {
    //   simpleField: String,
    //   item: Array<{
    //     linkId: String,
    //     item: Array<{
    //       linkId: String
    //     }>
    //   }>
    // }
    final StructType innerItemSchema = createStructType(List.of(
        createStructField("linkId", StringType, true)
    ));

    final StructType outerItemSchema = createStructType(List.of(
        createStructField("linkId", StringType, true),
        createStructField("item", createArrayType(innerItemSchema), true)
    ));

    final StructType schema = createStructType(List.of(
        createStructField("simpleField", StringType, true),
        createStructField("item", createArrayType(outerItemSchema), true)
    ));

    // Path: simpleField (simpleField:1) -> max = 1
    // Path: item -> linkId (item:1, linkId:1) -> max = 1
    // Path: item -> item -> linkId (item:2, linkId:1) -> max = 2
    // Overall max is 2
    assertEquals(2, SchemaUtils.computeMaxNestingLevel(schema));
  }

  @Test
  void testComplexSchemaWithMultipleRepeatedFields() {
    // Schema: {
    //   item: Array<{
    //     text: String,
    //     answer: Array<{
    //       item: Array<{
    //         text: String
    //       }>
    //     }>
    //   }>
    // }
    final StructType answerItemSchema = createStructType(List.of(
        createStructField("text", StringType, true)
    ));

    final StructType answerSchema = createStructType(List.of(
        createStructField("item", createArrayType(answerItemSchema), true)
    ));

    final StructType itemSchema = createStructType(List.of(
        createStructField("text", StringType, true),
        createStructField("answer", createArrayType(answerSchema), true)
    ));

    final StructType schema = createStructType(List.of(
        createStructField("item", createArrayType(itemSchema), true)
    ));

    // Path: item -> text (item:1, text:1) -> max = 1
    // Path: item -> answer -> item -> text (item:2, answer:1, text:1) -> max = 2
    // Overall max is 2
    assertEquals(2, SchemaUtils.computeMaxNestingLevel(schema));
  }

  @Test
  void testArrayOfPrimitives() {
    // Schema: { tags: Array<String> }
    final StructType schema = createStructType(List.of(
        createStructField("tags", createArrayType(StringType), true)
    ));

    // Path: tags (tags:1)
    // Max count is 1
    assertEquals(1, SchemaUtils.computeMaxNestingLevel(schema));
  }

  @Test
  void testMixedNestedAndFlatFields() {
    // Schema: {
    //   id: String,
    //   data: {
    //     value: String,
    //     nested: {
    //       value: String
    //     }
    //   }
    // }
    final StructType nestedSchema = createStructType(List.of(
        createStructField("value", StringType, true)
    ));

    final StructType dataSchema = createStructType(List.of(
        createStructField("value", StringType, true),
        createStructField("nested", nestedSchema, true)
    ));

    final StructType schema = createStructType(List.of(
        createStructField("id", StringType, true),
        createStructField("data", dataSchema, true)
    ));

    // Path: id (id:1) -> max = 1
    // Path: data -> value (data:1, value:1) -> max = 1
    // Path: data -> nested -> value (data:1, nested:1, value:1) -> max = 1
    // Note: "value" appears in different branches, but only once per path
    // Overall max is 1
    assertEquals(1, SchemaUtils.computeMaxNestingLevel(schema));
  }

  @Test
  void testSameFieldNameInSinglePath() {
    // Schema with the same field name appearing multiple times in a single path:
    // {
    //   data: {
    //     value: String,
    //     data: {
    //       value: String
    //     }
    //   }
    // }
    final StructType innerDataSchema = createStructType(List.of(
        createStructField("value", StringType, true)
    ));

    final StructType outerDataSchema = createStructType(List.of(
        createStructField("value", StringType, true),
        createStructField("data", innerDataSchema, true)
    ));

    final StructType schema = createStructType(List.of(
        createStructField("data", outerDataSchema, true)
    ));

    // Path: data -> value (data:1, value:1) -> max = 1
    // Path: data -> data -> value (data:2, value:1) -> max = 2
    // Overall max is 2 (for "data" appearing twice in same path)
    assertEquals(2, SchemaUtils.computeMaxNestingLevel(schema));
  }
}
