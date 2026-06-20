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

import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.lit;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import au.csiro.pathling.test.SpringBootUnitTest;
import jakarta.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Verifies the centralised safe-cast helper ({@link ColumnRepresentation#elementTryCast}). The
 * helper must return NULL (never raise) for out-of-range and non-conforming values, element-wise
 * for both array (plural) and scalar (singular) representations, and must behave identically under
 * {@code spark.sql.ansi.enabled} set to either {@code true} or {@code false}.
 *
 * @author John Grimes
 */
@SpringBootUnitTest
class ColumnRepresentationSafeCastTest {

  /** The canonical decimal type used by Pathling, permitting 26 integer digits. */
  private static final DataType DECIMAL_TYPE = DataTypes.createDecimalType(32, 6);

  private static final boolean[] ANSI_SETTINGS = {true, false};

  @Autowired SparkSession spark;

  private String originalAnsiSetting;

  @AfterEach
  void restoreAnsiSetting() {
    // Restore whatever the harness configured so other tests are unaffected by the toggling here.
    if (originalAnsiSetting != null) {
      spark.conf().set("spark.sql.ansi.enabled", originalAnsiSetting);
    }
  }

  /**
   * Builds the column under the given ANSI setting and materialises the single result row. The
   * column is constructed after the setting is applied so that any cast eval mode is resolved under
   * that setting.
   */
  @Nonnull
  private Row evalUnderAnsi(final boolean ansiEnabled, @Nonnull final Supplier<Column> column) {
    if (originalAnsiSetting == null) {
      originalAnsiSetting = spark.conf().get("spark.sql.ansi.enabled");
    }
    spark.conf().set("spark.sql.ansi.enabled", ansiEnabled);
    return spark.range(1).select(column.get().alias("v")).first();
  }

  @Nonnull
  private static ColumnRepresentation scalar(@Nonnull final Object value) {
    return new DefaultRepresentation(lit(value));
  }

  @Nonnull
  private static ColumnRepresentation arrayOf(@Nonnull final Object... values) {
    return new DefaultRepresentation(
        array(Arrays.stream(values).map(functions::lit).toArray(Column[]::new)));
  }

  @Test
  void scalarNonConformingStringToIntegerYieldsNull() {
    for (final boolean ansi : ANSI_SETTINGS) {
      final Row row =
          evalUnderAnsi(ansi, () -> scalar("abc").elementTryCast(DataTypes.IntegerType).getValue());
      assertNull(row.isNullAt(0) ? null : row.get(0), "non-conforming scalar string, ansi=" + ansi);
    }
  }

  @Test
  void scalarOutOfRangeDecimalYieldsNull() {
    // 1e30 has 31 integer digits, exceeding the 26 permitted by DECIMAL(32,6).
    for (final boolean ansi : ANSI_SETTINGS) {
      final Row row =
          evalUnderAnsi(ansi, () -> scalar("1e30").elementTryCast(DECIMAL_TYPE).getValue());
      assertNull(row.isNullAt(0) ? null : row.get(0), "out-of-range scalar decimal, ansi=" + ansi);
    }
  }

  @Test
  void scalarConformingValueIsRetained() {
    for (final boolean ansi : ANSI_SETTINGS) {
      final Row row =
          evalUnderAnsi(ansi, () -> scalar("42").elementTryCast(DataTypes.IntegerType).getValue());
      assertEquals(42, row.get(0), "conforming scalar, ansi=" + ansi);
    }
  }

  @Test
  void arrayNonConformingElementsYieldNullPerElementUnderBothSettings() {
    for (final boolean ansi : ANSI_SETTINGS) {
      final List<Object> result =
          evalUnderAnsi(
                  ansi,
                  () -> arrayOf("1", "abc", "3").elementTryCast(DataTypes.IntegerType).getValue())
              .getList(0);
      // The non-conforming element becomes NULL while conforming elements are retained. This
      // per-element behaviour is what keeps results identical across ANSI settings.
      assertEquals(Arrays.asList(1, null, 3), result, "array integer cast, ansi=" + ansi);
    }
  }

  @Test
  void arrayOutOfRangeDecimalYieldsNullPerElementUnderBothSettings() {
    for (final boolean ansi : ANSI_SETTINGS) {
      final List<Object> result =
          evalUnderAnsi(ansi, () -> arrayOf("1.5", "1e30").elementTryCast(DECIMAL_TYPE).getValue())
              .getList(0);
      assertEquals(2, result.size(), "array size, ansi=" + ansi);
      assertNotNull(result.get(0), "conforming decimal retained, ansi=" + ansi);
      assertNull(result.get(1), "out-of-range decimal nulled, ansi=" + ansi);
    }
  }
}
