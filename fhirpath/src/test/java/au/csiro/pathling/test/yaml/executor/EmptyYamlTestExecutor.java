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

package au.csiro.pathling.test.yaml.executor;

import au.csiro.pathling.test.yaml.resolver.ResolverBuilder;
import jakarta.annotation.Nonnull;
import lombok.Value;
import org.slf4j.Logger;

/**
 * Simple implementation of YamlTestExecutor for scenarios where no tests are available. Provides a
 * minimal implementation that logs the absence of tests and performs no validation.
 */
@Value(staticConstructor = "of")
public class EmptyYamlTestExecutor implements YamlTestExecutor {

  public void log(@Nonnull final Logger log) {
    log.info("No tests");
  }

  @Override
  public void check(@Nonnull final ResolverBuilder rb) {
    // Do nothing, as there are no tests to run.
  }

  @Override
  @Nonnull
  public String getDescription() {
    return "none";
  }
}
