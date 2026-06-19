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

package au.csiro.pathling;

import static org.junit.jupiter.api.Assertions.assertEquals;

import au.csiro.pathling.test.SpringBootUnitTest;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Verifies that the test harness resolves {@code spark.sql.ansi.enabled} consistently with the
 * {@code pathling.test.ansiEnabled} system property: ANSI is on by default (property unset) and off
 * only when the property is explicitly {@code false} (FR-009). This guards against the supported
 * default silently regressing.
 *
 * @author John Grimes
 */
@SpringBootUnitTest
class AnsiHarnessAssertionTest {

  @Autowired SparkSession spark;

  @Test
  void resolvedAnsiSettingMatchesProperty() {
    // The property defaults to unset, which the harness treats as ANSI-on.
    final String property = System.getProperty("pathling.test.ansiEnabled");
    final boolean expected = !"false".equalsIgnoreCase(property);
    final boolean actual = Boolean.parseBoolean(spark.conf().get("spark.sql.ansi.enabled"));
    assertEquals(
        expected,
        actual,
        "Resolved spark.sql.ansi.enabled must match the pathling.test.ansiEnabled property");
  }
}
