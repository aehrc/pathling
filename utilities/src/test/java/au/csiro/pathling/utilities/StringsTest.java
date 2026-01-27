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

package au.csiro.pathling.utilities;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.Test;

class StringsTest {

  @Test
  void testUnSingleQuote() {
    // Removes surrounding single quotes.
    assertEquals("hello", Strings.unSingleQuote("'hello'"));

    // Removes only leading quote.
    assertEquals("hello", Strings.unSingleQuote("'hello"));

    // Removes only trailing quote.
    assertEquals("hello", Strings.unSingleQuote("hello'"));

    // Leaves string unchanged when no surrounding quotes.
    assertEquals("hello", Strings.unSingleQuote("hello"));

    // Handles empty string.
    assertEquals("", Strings.unSingleQuote(""));

    // Preserves quotes embedded in the middle.
    assertEquals("hel'lo", Strings.unSingleQuote("'hel'lo'"));

    // Handles string that is just quotes.
    assertEquals("", Strings.unSingleQuote("''"));
  }

  @Test
  void testUnTickQuote() {
    // Removes surrounding tick quotes.
    assertEquals("hello", Strings.unTickQuote("`hello`"));

    // Removes only leading tick quote.
    assertEquals("hello", Strings.unTickQuote("`hello"));

    // Removes only trailing tick quote.
    assertEquals("hello", Strings.unTickQuote("hello`"));

    // Leaves string unchanged when no surrounding tick quotes.
    assertEquals("hello", Strings.unTickQuote("hello"));

    // Handles empty string.
    assertEquals("", Strings.unTickQuote(""));

    // Preserves tick quotes embedded in the middle.
    assertEquals("hel`lo", Strings.unTickQuote("`hel`lo`"));

    // Handles string that is just tick quotes.
    assertEquals("", Strings.unTickQuote("``"));
  }

  @Test
  void testParseCsvList() {
    assertEquals(Collections.emptyList(), Strings.parseCsvList("", Integer::parseInt));
    assertEquals(Collections.emptyList(), Strings.parseCsvList(", ,, ", Integer::parseInt));
    assertEquals(Arrays.asList(1, 2, 4), Strings.parseCsvList(", 1, 2, , 4 , ", Integer::parseInt));
  }
}
