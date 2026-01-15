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

package au.csiro.pathling.test.yaml.executor;

import au.csiro.pathling.test.yaml.resolver.ResolverBuilder;
import jakarta.annotation.Nonnull;
import org.slf4j.Logger;

/** Interface defining the contract for test case execution. */
public interface YamlTestExecutor {

  /**
   * Logs the test case details.
   *
   * @param log The logger instance to use
   */
  void log(@Nonnull Logger log);

  /**
   * Executes the test case validation.
   *
   * @param rb The resolver builder to use for resource resolution
   */
  void check(@Nonnull final ResolverBuilder rb);

  /**
   * Gets a human-readable description of the test case.
   *
   * @return The test case description
   */
  @Nonnull
  String getDescription();
}
