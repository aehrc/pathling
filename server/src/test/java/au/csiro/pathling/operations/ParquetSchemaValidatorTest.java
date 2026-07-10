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

package au.csiro.pathling.operations;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import au.csiro.pathling.errors.InvalidUserInputError;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ParquetSchemaValidator}. These tests exercise the recursive schema walk and
 * the construction of the user-facing error message directly against Spark {@link StructType}
 * schemas, with no Spark session required.
 *
 * @author John Grimes
 */
class ParquetSchemaValidatorTest {

  @Test
  void cleanSchemaPasses() {
    // A schema with only concrete, writable types must not throw.
    final StructType schema =
        new StructType()
            .add("id", DataTypes.IntegerType, false)
            .add("name", DataTypes.StringType, true);

    assertThatCode(() -> ParquetSchemaValidator.validateSchemaForParquet(schema))
        .doesNotThrowAnyException();
  }

  @Test
  void singleTopLevelVoidColumnThrowsNamingIt() {
    // A single top-level NullType column must be rejected and named in the message.
    final StructType schema =
        new StructType()
            .add("id", DataTypes.IntegerType, false)
            .add("foo", DataTypes.NullType, true);

    assertThatThrownBy(() -> ParquetSchemaValidator.validateSchemaForParquet(schema))
        .isInstanceOf(InvalidUserInputError.class)
        .hasMessageContaining("'foo'");
  }

  @Test
  void multipleVoidColumnsAreAllNamedInOneMessage() {
    // Every offending column must appear in a single message, not just the first.
    final StructType schema =
        new StructType()
            .add("foo", DataTypes.NullType, true)
            .add("bar", DataTypes.IntegerType, false)
            .add("baz", DataTypes.NullType, true);

    assertThatThrownBy(() -> ParquetSchemaValidator.validateSchemaForParquet(schema))
        .isInstanceOf(InvalidUserInputError.class)
        .hasMessageContaining("'foo'")
        .hasMessageContaining("'baz'");
  }

  @Test
  void voidNestedInStructIsReportedWithDottedPath() {
    // A NullType nested inside a struct is reported using dot-separated field paths.
    final StructType nested = new StructType().add("code", DataTypes.NullType, true);
    final StructType schema = new StructType().add("details", nested, true);

    assertThatThrownBy(() -> ParquetSchemaValidator.validateSchemaForParquet(schema))
        .isInstanceOf(InvalidUserInputError.class)
        .hasMessageContaining("'details.code'");
  }

  @Test
  void voidNestedInArrayElementIsReportedWithBracketPath() {
    // A NullType reached through an array element is reported with a "[]" segment.
    final StructType elementType = new StructType().add("code", DataTypes.NullType, true);
    final StructType schema =
        new StructType().add("details", DataTypes.createArrayType(elementType, true), true);

    assertThatThrownBy(() -> ParquetSchemaValidator.validateSchemaForParquet(schema))
        .isInstanceOf(InvalidUserInputError.class)
        .hasMessageContaining("'details[].code'");
  }

  @Test
  void voidAsDirectArrayElementIsReportedWithBracketPath() {
    // An array whose element type is itself VOID (e.g. an array() literal) is reported.
    final StructType schema =
        new StructType().add("tags", DataTypes.createArrayType(DataTypes.NullType, true), true);

    assertThatThrownBy(() -> ParquetSchemaValidator.validateSchemaForParquet(schema))
        .isInstanceOf(InvalidUserInputError.class)
        .hasMessageContaining("'tags[]'");
  }

  @Test
  void voidAsMapKeyIsReportedWithKeyPath() {
    // A NullType map key is reported with a ".key" segment.
    final StructType schema =
        new StructType()
            .add(
                "attributes",
                DataTypes.createMapType(DataTypes.NullType, DataTypes.StringType, true),
                true);

    assertThatThrownBy(() -> ParquetSchemaValidator.validateSchemaForParquet(schema))
        .isInstanceOf(InvalidUserInputError.class)
        .hasMessageContaining("'attributes.key'");
  }

  @Test
  void voidAsMapValueIsReportedWithValuePath() {
    // A NullType map value is reported with a ".value" segment.
    final StructType schema =
        new StructType()
            .add(
                "attributes",
                DataTypes.createMapType(DataTypes.StringType, DataTypes.NullType, true),
                true);

    assertThatThrownBy(() -> ParquetSchemaValidator.validateSchemaForParquet(schema))
        .isInstanceOf(InvalidUserInputError.class)
        .hasMessageContaining("'attributes.value'");
  }

  @Test
  void messageContainsBothRemediations() {
    // The message must offer both remediations: an explicit CAST and a different output format.
    final StructType schema = new StructType().add("foo", DataTypes.NullType, true);

    assertThatThrownBy(() -> ParquetSchemaValidator.validateSchemaForParquet(schema))
        .isInstanceOf(InvalidUserInputError.class)
        .hasMessageContaining("CAST")
        .hasMessageContaining("output format");
  }
}
