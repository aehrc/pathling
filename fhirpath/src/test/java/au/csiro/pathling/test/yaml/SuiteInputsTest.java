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

import static org.assertj.core.api.Assertions.assertThat;

import au.csiro.pathling.test.yaml.annotations.YamlTestConfiguration;
import org.junit.jupiter.api.Test;

/**
 * Tests that the inputs of a suite are read from its resource base as {@code
 * YamlTestArgumentProvider} reads it. This class is itself the suite with a blank resource base.
 *
 * @author Piotr Szul
 */
@YamlTestConfiguration(resourceBase = " ")
class SuiteInputsTest {

  @Test
  void suiteWithoutAConfigurationHasNoResourceFiles() {
    assertThat(SuiteInputs.resourceBase(Object.class)).isEmpty();
    assertThat(SuiteInputs.documentsOf(Object.class)).isEmpty();
  }

  @Test
  void suiteWithABlankResourceBaseHasNoResourceFiles() {
    assertThat(SuiteInputs.resourceBase(SuiteInputsTest.class)).isEmpty();
    assertThat(SuiteInputs.documentsOf(SuiteInputsTest.class)).isEmpty();
  }
}
