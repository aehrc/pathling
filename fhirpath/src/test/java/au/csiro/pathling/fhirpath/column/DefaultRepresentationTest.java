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

package au.csiro.pathling.fhirpath.column;

import au.csiro.pathling.test.SpringBootUnitTest;
import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;


@SpringBootUnitTest
class DefaultRepresentationTest {

  class ColumnAsserts {

    final List<Column> columns = new ArrayList<>();
    final List<Object> expectedValues = new ArrayList<>();

    @Nonnull
    ColumnAsserts assertNull(@Nonnull final ColumnRepresentation column) {
      columns.add(column.getValue());
      expectedValues.add(null);
      return this;
    }

    @Nonnull
    ColumnAsserts assertEquals(@Nonnull final Object expectedValue,
        @Nonnull final ColumnRepresentation column) {
      columns.add(column.getValue());
      expectedValues.add(expectedValue);
      return this;
    }

    @Nonnull
    ColumnAsserts assertEquals(@Nonnull final ColumnRepresentation expectedValue,
        @Nonnull final ColumnRepresentation column) {
      columns.add(column.getValue());
      columns.add(expectedValue.getValue());
      expectedValues.add("COMPARE_COLUMNS");
      expectedValues.add("COMPARE_COLUMNS");
      return this;
    }

    void check() {
      final Row result = spark.range(1).select(columns.toArray(Column[]::new)).first();

      int columnIndex = 0;
      for (int i = 0; i < expectedValues.size(); i++) {
        final Object expected = expectedValues.get(i);

        if ("COMPARE_COLUMNS".equals(expected)) {
          final Object actual1 = result.get(columnIndex++);
          final Object actual2 = result.get(columnIndex++);
          Assertions.assertEquals(actual1, actual2, String.format(
              "Column comparison failed: expected columns to be equal, but got %s and %s",
              actual1, actual2));
          i++;
        } else {
          final Object actual = result.get(columnIndex++);
          Assertions.assertEquals(expected, actual,
              String.format("Assertion %d failed: expected <%s> but was <%s>", i, expected,
                  actual));
        }
      }
    }
  }

  @Autowired
  SparkSession spark;

  @Nonnull
  public static DefaultRepresentation nullValue() {
    return new DefaultRepresentation(functions.lit(null));
  }

  @Nonnull
  public static DefaultRepresentation valueOf(@Nonnull final Object value) {
    return new DefaultRepresentation(functions.lit(value));
  }


  @Nonnull
  public static DefaultRepresentation nullArray() {
    return new DefaultRepresentation(
        functions.lit(null).cast(DataTypes.createArrayType(DataTypes.NullType)));
  }

  @Nonnull
  public static DefaultRepresentation emptyArray() {
    return new DefaultRepresentation(functions.array());
  }

  @Nonnull
  public static DefaultRepresentation arrayOf(@Nonnull final Object... values) {
    return new DefaultRepresentation(functions.array(
        Stream.of(values).map(v -> valueOf(v).getValue()).toArray(Column[]::new)));
  }

  @Nonnull
  public static DefaultRepresentation arrayOfOne(@Nonnull final Object value) {
    return new DefaultRepresentation(functions.array(valueOf(value).getValue()));
  }


  @Test
  void testSingular() {

    new ColumnAsserts()
        .assertNull(nullValue().singular())
        .assertNull(emptyArray().singular())
        .assertEquals(13, valueOf(13).singular())
        .assertEquals("a", arrayOfOne("a").singular())
        .check();
  }

  @Test
  void testFirst() {
    new ColumnAsserts()
        .assertNull(nullValue().first())
        .assertEquals(13, valueOf(13).first())
        .assertNull(nullArray().first())
        .assertNull(emptyArray().first())
        .assertEquals("a", arrayOf("a", "b").first())
        .check();
  }

  @Test
  void testLast() {
    new ColumnAsserts()
        .assertNull(nullValue().last())
        .assertEquals(17, valueOf(17).last())
        .assertNull(nullArray().last())
        .assertNull(emptyArray().last())
        .assertEquals("b", arrayOf("a", "b").last())
        .check();
  }

  @Test
  void testCount() {
    new ColumnAsserts()
        .assertEquals(0, nullValue().count())
        .assertEquals(1, valueOf(17).count())
        .assertEquals(0, nullArray().count())
        .assertEquals(0, emptyArray().count())
        .assertEquals(2, arrayOf("a", "b").count())
        .check();
  }

  @Test
  void testSum() {
    new ColumnAsserts()
        .assertEquals(0, nullValue().sum())
        .assertEquals(17, valueOf(17).sum())
        .assertEquals(0, nullArray().sum())
        .assertEquals(0, emptyArray().sum())
        .assertEquals(6, arrayOf(1, 2, 3).sum())
        .check();
  }

  @Test
  void testNot() {
    new ColumnAsserts()
        .assertNull(nullValue().not())
        .assertEquals(true, valueOf(false).not())
        .assertEquals(false, valueOf(true).not())
        .assertNull(nullArray().not())
        .assertEquals(emptyArray(), emptyArray().not())
        .assertEquals(arrayOf(false, true), arrayOf(true, false).not())
        .check();
  }

  @Test
  void testEmpty() {
    new ColumnAsserts()
        .assertEquals(true, nullValue().isEmpty())
        .assertEquals(false, valueOf(17).isEmpty())
        .assertEquals(true, nullArray().isEmpty())
        .assertEquals(true, emptyArray().isEmpty())
        .assertEquals(false, arrayOf(1, 2, 3).isEmpty())
        .check();
  }

  @Test
  void testMax() {
    new ColumnAsserts()
        .assertNull(nullValue().max())
        .assertEquals(17, valueOf(17).max())
        .assertNull(nullArray().max())
        .assertNull(emptyArray().max())
        .assertEquals(true, arrayOf(true, false).max())
        .check();
  }

  @Test
  void testMin() {
    new ColumnAsserts()
        .assertNull(nullValue().min())
        .assertEquals(17, valueOf(17).min())
        .assertNull(nullArray().min())
        .assertNull(emptyArray().min())
        .assertEquals(false, arrayOf(true, false).min())
        .check();
  }

  @Test
  void testAllTrue() {
    new ColumnAsserts()
        .assertEquals(true, nullValue().allTrue())
        .assertEquals(true, valueOf(true).allTrue())
        .assertEquals(false, valueOf(false).allTrue())
        .assertEquals(true, nullArray().allTrue())
        .assertEquals(true, emptyArray().allTrue())
        .assertEquals(true, arrayOf(true, true).allTrue())
        .assertEquals(false, arrayOf(false, true).allTrue())
        .assertEquals(false, arrayOf(false, false).allTrue())
        .check();
  }

  @Test
  void testAllFalse() {
    new ColumnAsserts()
        .assertEquals(true, nullValue().allFalse())
        .assertEquals(false, valueOf(true).allFalse())
        .assertEquals(true, valueOf(false).allFalse())
        .assertEquals(true, nullArray().allFalse())
        .assertEquals(true, emptyArray().allFalse())
        .assertEquals(false, arrayOf(true, true).allFalse())
        .assertEquals(false, arrayOf(false, true).allFalse())
        .assertEquals(true, arrayOf(false, false).allFalse())
        .check();
  }

  @Test
  void testAnyTrue() {
    new ColumnAsserts()
        .assertEquals(false, nullValue().anyTrue())
        .assertEquals(true, valueOf(true).anyTrue())
        .assertEquals(false, valueOf(false).anyTrue())
        .assertEquals(false, nullArray().anyTrue())
        .assertEquals(false, emptyArray().anyTrue())
        .assertEquals(true, arrayOf(true, true).anyTrue())
        .assertEquals(true, arrayOf(false, true).anyTrue())
        .assertEquals(false, arrayOf(false, false).anyTrue())
        .check();
  }

  @Test
  void testAnyFalse() {
    new ColumnAsserts()
        .assertEquals(false, nullValue().anyFalse())
        .assertEquals(false, valueOf(true).anyFalse())
        .assertEquals(true, valueOf(false).anyFalse())
        .assertEquals(false, nullArray().anyFalse())
        .assertEquals(false, emptyArray().anyFalse())
        .assertEquals(false, arrayOf(true, true).anyFalse())
        .assertEquals(true, arrayOf(false, true).anyFalse())
        .assertEquals(true, arrayOf(false, false).anyFalse())
        .check();
  }

}
  
 
