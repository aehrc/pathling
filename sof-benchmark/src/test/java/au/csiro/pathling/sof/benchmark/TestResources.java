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

package au.csiro.pathling.sof.benchmark;

import jakarta.annotation.Nonnull;
import java.net.URISyntaxException;
import java.nio.file.Path;

/** Resolves the contract-v2 test fixtures on the test classpath as filesystem paths. */
final class TestResources {

  private TestResources() {}

  /**
   * Resolves a classpath resource to a filesystem path.
   *
   * @param resource the absolute classpath resource path
   * @return the resolved filesystem path
   */
  @Nonnull
  static Path path(@Nonnull final String resource) {
    try {
      return Path.of(TestResources.class.getResource(resource).toURI());
    } catch (final URISyntaxException e) {
      throw new IllegalStateException("Failed to resolve test resource: " + resource, e);
    }
  }

  /**
   * Returns the fixture benchmark file.
   *
   * @return the benchmark file path
   */
  @Nonnull
  static Path benchmarkFile() {
    return path("/contract-v2/clinical-flat.json");
  }

  /**
   * Returns the root of the fixture materialized data tree ({@code <dataRoot>/<name>/<version>/…}).
   *
   * @return the data root path
   */
  @Nonnull
  static Path dataRoot() {
    return path("/contract-v2/data");
  }
}
