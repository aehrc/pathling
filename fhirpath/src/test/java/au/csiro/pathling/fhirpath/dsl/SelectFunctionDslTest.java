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

package au.csiro.pathling.fhirpath.dsl;

import au.csiro.pathling.test.dsl.FhirPathDslTestBase;
import au.csiro.pathling.test.dsl.FhirPathTest;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;

/** Tests for the FHIRPath select() function. */
public class SelectFunctionDslTest extends FhirPathDslTestBase {

  @FhirPathTest
  public Stream<DynamicTest> testSelectSimpleProperty() {
    return builder()
        .withSubject(
            sb ->
                sb.elementArray(
                    "people",
                    person1 -> person1.string("name", "Alice").integer("age", 25),
                    person2 -> person2.string("name", "Bob").integer("age", 40),
                    person3 -> person3.string("name", "Charlie").integer("age", 35)))
        .group("select() simple property projection")
        .testEquals(
            List.of("Alice", "Bob", "Charlie"),
            "people.select(name)",
            "select() projects a simple string property from each element")
        .testEquals(
            List.of(25, 40, 35),
            "people.select(age)",
            "select() projects a simple integer property from each element")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testSelectFlatteningMultiValued() {
    return builder()
        .withSubject(
            sb ->
                sb.elementArray(
                    "contacts",
                    c1 -> c1.string("name", "Alice").stringArray("phones", "111", "222"),
                    c2 -> c2.string("name", "Bob").stringArray("phones", "333"),
                    c3 -> c3.string("name", "Charlie").stringArray("phones", "444", "555", "666")))
        .group("select() flattening of multi-valued results")
        .testEquals(
            List.of("111", "222", "333", "444", "555", "666"),
            "contacts.select(phones)",
            "select() flattens multi-valued projection results into a single collection")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testSelectEmptyResults() {
    return builder()
        .withSubject(
            sb ->
                sb.elementArray(
                    "items",
                    item1 -> item1.string("label", "first").stringEmpty("note"),
                    item2 -> item2.string("label", "second").string("note", "important"),
                    item3 -> item3.string("label", "third").stringEmpty("note")))
        .group("select() empty projection results")
        .testEquals(
            "important",
            "items.select(note)",
            "select() omits elements whose projection evaluates to empty")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testSelectEmptyInput() {
    return builder()
        .withSubject(sb -> sb.string("dummy", "value"))
        .group("select() empty input collection")
        .testEmpty("{}.select($this)", "select() on an empty collection returns empty")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testSelectExpression() {
    return builder()
        .withSubject(
            sb ->
                sb.stringArray("names", "Alice", "Bob", "Charlie")
                    .integerArray("numbers", 1, 2, 3, 4, 5))
        .group("select() with expressions")
        .testEquals(
            List.of(true, true, true),
            "names.select($this.exists())",
            "select() evaluates an expression for each element")
        .testEquals(
            List.of(true, true, true, true, true),
            "numbers.select($this > 0)",
            "select() evaluates a comparison expression for each element")
        .build();
  }
}
