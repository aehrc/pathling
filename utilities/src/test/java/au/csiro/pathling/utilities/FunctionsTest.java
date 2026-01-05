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

import org.junit.jupiter.api.Test;

class FunctionsTest {

  public static String biFunction(final boolean a, final int b) {
    return a + "_" + b;
  }

  @Test
  void curried() {
    assertEquals("true_7", Functions.curried(FunctionsTest::biFunction).apply(true).apply(7));
  }

  @Test
  void backCurried() {
    assertEquals(
        "false_13", Functions.backCurried(FunctionsTest::biFunction).apply(13).apply(false));
  }
}
