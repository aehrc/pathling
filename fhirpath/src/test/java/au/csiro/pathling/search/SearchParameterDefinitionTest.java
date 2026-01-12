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

package au.csiro.pathling.search;

import static au.csiro.pathling.search.SearchParameterType.DATE;
import static au.csiro.pathling.search.SearchParameterType.TOKEN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link SearchParameterDefinition}.
 */
class SearchParameterDefinitionTest {

  @Test
  void testSingleExpressionConstructor() {
    final var def = new SearchParameterDefinition("gender", TOKEN, "Patient.gender");

    assertEquals("gender", def.code());
    assertEquals(TOKEN, def.type());
    assertEquals(List.of("Patient.gender"), def.expressions());
  }

  @Test
  void testMultipleExpressionsConstructor() {
    final var def = new SearchParameterDefinition("date", DATE, List.of(
        "Observation.effective.ofType(dateTime)",
        "Observation.effective.ofType(Period)",
        "Observation.effective.ofType(instant)"
    ));

    assertEquals("date", def.code());
    assertEquals(DATE, def.type());
    assertEquals(3, def.expressions().size());
    assertEquals("Observation.effective.ofType(dateTime)", def.expressions().get(0));
    assertEquals("Observation.effective.ofType(Period)", def.expressions().get(1));
    assertEquals("Observation.effective.ofType(instant)", def.expressions().get(2));
  }

  @Test
  void testExpressionsListIsImmutableFromConstructor() {
    final var originalList = new ArrayList<>(List.of("expr1", "expr2"));
    final var def = new SearchParameterDefinition("test", DATE, originalList);

    // Modify the original list
    originalList.add("expr3");

    // The definition should be unaffected
    assertEquals(2, def.expressions().size());
    assertEquals(List.of("expr1", "expr2"), def.expressions());
  }

  @Test
  void testExpressionsListIsUnmodifiable() {
    final var def = new SearchParameterDefinition("test", DATE, List.of("expr1", "expr2"));
    final var expressions = def.expressions();

    // Attempting to modify the returned list should throw
    assertThrows(UnsupportedOperationException.class, () -> expressions.add("expr3"));
  }

  @Test
  void testRecordEquality() {
    final var def1 = new SearchParameterDefinition("gender", TOKEN, "Patient.gender");
    final var def2 = new SearchParameterDefinition("gender", TOKEN, "Patient.gender");

    assertEquals(def1, def2);
    assertEquals(def1.hashCode(), def2.hashCode());
  }

  @Test
  void testRecordEqualityWithList() {
    final var def1 = new SearchParameterDefinition("date", DATE, List.of("expr1", "expr2"));
    final var def2 = new SearchParameterDefinition("date", DATE, List.of("expr1", "expr2"));

    assertEquals(def1, def2);
    assertEquals(def1.hashCode(), def2.hashCode());
  }
}
