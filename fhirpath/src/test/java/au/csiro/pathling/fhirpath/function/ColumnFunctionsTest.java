/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.fhirpath.function;

import static au.csiro.pathling.fhirpath.function.ColumnFunctions.count;
import static au.csiro.pathling.fhirpath.function.ColumnFunctions.empty;
import static au.csiro.pathling.fhirpath.function.ColumnFunctions.first;
import static au.csiro.pathling.fhirpath.function.ColumnFunctions.last;
import static au.csiro.pathling.fhirpath.function.ColumnFunctions.not;
import static au.csiro.pathling.fhirpath.function.ColumnFunctions.singular;
import static au.csiro.pathling.fhirpath.function.ColumnFunctions.sum;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.test.SpringBootUnitTest;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;


@SpringBootUnitTest
class ColumnFunctionsTest {


  // TODO: improve to check the actual values returned.
  class ColumnAsserts {

    final List<Column> tests = new ArrayList<>();

    ColumnAsserts assertNull(Column column) {
      tests.add(column.isNull());
      return this;
    }

    ColumnAsserts assertEquals(Object value, Column column) {
      tests.add(column.equalTo(value));
      return this;
    }

    void check() {
      final Row result = spark.range(1).select(tests.toArray(Column[]::new))
          .first();
      for (int i = 0; i < result.size(); i++) {
        assertTrue(result.getBoolean(i), "Test " + i + " failed: " + tests.get(i));
      }
      System.out.println(result);
    }
  }

  @Autowired
  SparkSession spark;

  @Nonnull
  public static Column nullValue() {
    return functions.lit(null);
  }

  @Nonnull
  public static Column valueOf(Object value) {
    return functions.lit(value);
  }


  @Nonnull
  public static Column nullArray() {
    return functions.lit(null).cast(DataTypes.createArrayType(DataTypes.NullType));
  }

  @Nonnull
  public static Column emptyArray() {
    return functions.array();
  }

  @Nonnull
  public static Column arrayOf(Object... values) {
    return functions.array(
        Stream.of(values).map(ColumnFunctionsTest::valueOf).toArray(Column[]::new));
  }

  @Nonnull
  public static Column arrayOfOne(Object value) {
    return functions.array(valueOf(value));
  }


  @Test
  void testSingular() {

    new ColumnAsserts()
        .assertNull(singular(nullValue()))
        .assertNull(singular(emptyArray()))
        .assertEquals(13, singular(valueOf(13)))
        .assertEquals("a", singular(arrayOfOne("a")))
        .check();

    // final SparkException ex = assertThrows(SparkException.class, () ->
    //     spark.range(1).select(
    //         ColumnHelpers.singular(functions.array(functions.lit("a"), functions.lit("b")))
    //     ).collect());
    // System.out.println(ex.getCause().getMessage());

  }

  @Test
  void testFirst() {
    new ColumnAsserts()
        .assertNull(first(nullValue()))
        .assertEquals(13, first(valueOf(13)))
        .assertNull(first(nullArray()))
        .assertNull(first(emptyArray()))
        .assertEquals("a", first(arrayOf("a", "b")))
        .check();
  }

  @Test
  void testLast() {
    new ColumnAsserts()
        .assertNull(last(nullValue()))
        .assertEquals(17, last(valueOf(17)))
        .assertNull(last(nullArray()))
        .assertNull(last(emptyArray()))
        .assertEquals("b", last(arrayOf("a", "b")))
        .check();
  }

  @Test
  void testCount() {
    new ColumnAsserts()
        .assertEquals(0, count(nullValue()))
        .assertEquals(1, count(valueOf(17)))
        .assertEquals(0, count(nullArray()))
        .assertEquals(0, count(emptyArray()))
        .assertEquals(2, count(arrayOf("a", "b")))
        .check();
  }

  @Test
  void testSum() {
    new ColumnAsserts()
        .assertEquals(0, sum(nullValue()))
        .assertEquals(17, sum(valueOf(17)))
        .assertEquals(0, sum(nullArray()))
        .assertEquals(0, sum(emptyArray()))
        .assertEquals(6, sum(arrayOf(1, 2, 3)))
        .check();
  }


  @Test
  void testNot() {
    new ColumnAsserts()
        .assertNull(not(nullValue()))
        .assertEquals(true, not(valueOf(false)))
        .assertEquals(false, not(valueOf(true)))
        .assertNull(not(nullArray()))
        .assertEquals(emptyArray(), not(emptyArray()))
        .assertEquals(arrayOf(false, true), not(arrayOf(true, false)))
        .check();
  }

  @Test
  void testEmpty() {
    new ColumnAsserts()
        .assertEquals(true, empty(nullValue()))
        .assertEquals(false, empty(valueOf(17)))
        .assertEquals(true, empty(nullArray()))
        .assertEquals(true, empty(emptyArray()))
        .assertEquals(false, empty(arrayOf(1, 2, 3)))
        .check();
  }


}
  
 
