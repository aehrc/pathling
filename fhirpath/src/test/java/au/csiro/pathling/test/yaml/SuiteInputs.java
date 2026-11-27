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
package au.csiro.pathling.test.yaml;

import jakarta.annotation.Nonnull;
import java.util.List;

/**
 * The input resources of a YAML suite (path 1 of R11, FR-021b), which its suite schemas are
 * inferred from. A suite is a test class: its inputs are every file in the resource directory its
 * {@code YamlTestConfiguration} names, and the subject of each of its YAML files that is a FHIR
 * resource.
 *
 * @author Piotr Szul
 */
public final class SuiteInputs {

  private SuiteInputs() {}

  /**
   * Returns the input resources of the suite a test class runs.
   *
   * @param suiteClass the test class
   * @return the suite's input resources, one FHIR JSON document each, of any type
   */
  @Nonnull
  public static List<String> documentsOf(@Nonnull final Class<?> suiteClass) {
    throw new UnsupportedOperationException(
        "The inputs of suite " + suiteClass.getSimpleName() + " are not gathered until T047");
  }
}
