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

package au.csiro.pathling.fhirpath.yaml;

import au.csiro.pathling.test.yaml.YamlCachedTestBase;
import au.csiro.pathling.test.yaml.annotations.YamlTest;
import au.csiro.pathling.test.yaml.annotations.YamlTestConfiguration;
import au.csiro.pathling.test.yaml.executor.YamlTestExecutor;
import jakarta.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@YamlTestConfiguration(
    resourceBase = "fhirpath-ptl/resources"
)
public class YamlFhirpathTest extends YamlCachedTestBase {

  @YamlTest("fhirpath-ptl/cases/math.yaml")
  void testMath(@Nonnull final YamlTestExecutor testCase) {
    run(testCase);
  }

  @YamlTest("fhirpath-ptl/cases/search-params.yaml")
  void testSearchParams(@Nonnull final YamlTestExecutor testCase) {
    run(testCase);
  }
  
  @YamlTest("fhirpath-ptl/cases/datetime_comparison.yaml")
  void testDateTimeComparison(@Nonnull final YamlTestExecutor testCase) {
    run(testCase);
  }

  @YamlTest("fhirpath-ptl/cases/repeat_all.yaml")
  void testRepeatAll(@Nonnull final YamlTestExecutor testCase) {
    run(testCase);
  }
}
