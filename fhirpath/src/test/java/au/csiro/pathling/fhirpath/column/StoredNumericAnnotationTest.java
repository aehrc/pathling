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

package au.csiro.pathling.fhirpath.column;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.utilities.SystemDecimal;
import jakarta.annotation.Nonnull;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Tests that a numeric annotation stored at a type other than the engine's System.Decimal is read
 * as System.Decimal, for a singular and a repeating decimal, nested and at the root (FR-022).
 *
 * <p>A value that the stored type holds exactly but System.Decimal does not is rounded, or is null
 * where it is out of range, as decoding its text would make it. No R4 element is a repeating
 * decimal, so the data is synthetic.
 */
@SpringBootUnitTest
class StoredNumericAnnotationTest {

  @Autowired SparkSession spark;

  @Nonnull
  static Stream<Arguments> cases() {
    return Stream.of(
        // Rounded to the six fractional digits System.Decimal holds.
        arguments("decimal(38,8)", "0.12345678", "0.123457"),
        // Beyond the 32 integer digits System.Decimal holds.
        arguments("decimal(38,0)", "1e33", null));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void aSingularDecimalNested(
      @Nonnull final String type, @Nonnull final String text, final String expected) {
    final Dataset<Row> data =
        spark.sql(
            "SELECT named_struct('x', '"
                + text
                + "', '_x', named_struct('__numeric', "
                + stored(text, type)
                + ")) AS s");
    final ColumnRepresentation decimal =
        new DefaultRepresentation(data.col("s"))
            .traverse("x", Optional.of(FHIRDefinedType.DECIMAL), DataTypes.StringType);
    assertDecimals(data, decimal, false, expected, type);
  }

  @ParameterizedTest
  @MethodSource("cases")
  void aSingularDecimalAtTheRoot(
      @Nonnull final String type, @Nonnull final String text, final String expected) {
    final Dataset<Row> data =
        spark.sql(
            "SELECT 'r' AS id, '"
                + text
                + "' AS x, named_struct('__numeric', "
                + stored(text, type)
                + ") AS _x");
    final ColumnRepresentation decimal =
        ResourceRepresentation.withIdColumn()
            .traverse("x", Optional.of(FHIRDefinedType.DECIMAL), DataTypes.StringType);
    assertDecimals(data, decimal, false, expected, type);
  }

  @ParameterizedTest
  @MethodSource("cases")
  void aRepeatingDecimalNested(
      @Nonnull final String type, @Nonnull final String text, final String expected) {
    final Dataset<Row> data =
        spark.sql(
            "SELECT named_struct('x', array('2.5', '"
                + text
                + "'), '_x', array(named_struct('__numeric', "
                + stored("2.5", type)
                + "),"
                + " named_struct('__numeric', "
                + stored(text, type)
                + "))) AS s");
    final ColumnRepresentation decimals =
        new DefaultRepresentation(data.col("s"))
            .traverse(
                "x",
                Optional.of(FHIRDefinedType.DECIMAL),
                DataTypes.createArrayType(DataTypes.StringType));
    assertDecimals(data, decimals, true, expected, type);
  }

  @ParameterizedTest
  @MethodSource("cases")
  void aRepeatingDecimalAtTheRoot(
      @Nonnull final String type, @Nonnull final String text, final String expected) {
    final Dataset<Row> data =
        spark.sql(
            "SELECT 'r' AS id, array('2.5', '"
                + text
                + "') AS x, array(named_struct('__numeric', "
                + stored("2.5", type)
                + "),"
                + " named_struct('__numeric', "
                + stored(text, type)
                + ")) AS _x");
    final ColumnRepresentation decimals =
        ResourceRepresentation.withIdColumn()
            .traverse(
                "x",
                Optional.of(FHIRDefinedType.DECIMAL),
                DataTypes.createArrayType(DataTypes.StringType));
    assertDecimals(data, decimals, true, expected, type);
  }

  /** Returns the SQL of a value stored as an annotation at a type. */
  @Nonnull
  private static String stored(@Nonnull final String value, @Nonnull final String type) {
    return "cast(" + value + " as " + type + ")";
  }

  /**
   * Asserts that a traversal is System.Decimal, and holds the expected value. A repeating decimal
   * holds a first value of 2.5 at the same stored type, which is 3 where that type has no
   * fractional digits.
   */
  private static void assertDecimals(
      @Nonnull final Dataset<Row> data,
      @Nonnull final ColumnRepresentation decimals,
      final boolean repeating,
      final String expected,
      @Nonnull final String storedType) {
    final Dataset<Row> result = data.select(decimals.getValue().alias("d"));
    final DataType type = result.schema().apply("d").dataType();
    final BigDecimal value = expected == null ? null : new BigDecimal(expected);
    if (repeating) {
      assertThat(type).isEqualTo(DataTypes.createArrayType(SystemDecimal.TYPE));
      assertThat(result.first().<BigDecimal>getList(0))
          .containsExactlyElementsOf(
              Arrays.asList(
                  new BigDecimal(storedType.endsWith(",0)") ? "3.000000" : "2.500000"), value));
    } else {
      assertThat(type).isEqualTo(SystemDecimal.TYPE);
      assertThat(result.first().<BigDecimal>getAs(0)).isEqualTo(value);
    }
  }
}
