/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright 2018-2026 Commonwealth Scientific
 * and Industrial Research Organisation (CSIRO) ABN 41 687 119 230.
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
package au.csiro.pathling.encoders;

import jakarta.annotation.Nonnull;
import org.apache.spark.sql.SparkSession;

/**
 * Test support for running the {@code encoders} suite under either {@code spark.sql.ansi.enabled}
 * setting. Mirrors the toggle in the {@code fhirpath} test harness so the full suite can be run
 * once under ANSI-off to confirm encoding is independent of the setting (FR-011).
 *
 * @author John Grimes
 */
public final class AnsiTestSupport {

  private AnsiTestSupport() {}

  /**
   * Disables ANSI mode on the session when {@code pathling.test.ansiEnabled} is set to {@code
   * false}; otherwise leaves the session at the Spark 4 default (ANSI on).
   *
   * @param spark the session to configure
   * @return the same session, for fluent use directly on a {@code getOrCreate()} call
   */
  @Nonnull
  public static SparkSession configureAnsiMode(@Nonnull final SparkSession spark) {
    if ("false".equalsIgnoreCase(System.getProperty("pathling.test.ansiEnabled"))) {
      spark.conf().set("spark.sql.ansi.enabled", false);
    }
    return spark;
  }
}
