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

package au.csiro.pathling.test.dsl;

import static au.csiro.pathling.test.yaml.YamlTestDefinition.TestCase.ANY_ERROR;
import static java.util.Objects.nonNull;

import au.csiro.pathling.fhirpath.evaluation.DatasetEvaluator;
import au.csiro.pathling.test.yaml.YamlTestDefinition;
import au.csiro.pathling.test.yaml.executor.DefaultYamlTestExecutor;
import au.csiro.pathling.test.yaml.executor.YamlTestExecutor;
import au.csiro.pathling.test.yaml.resolver.RuntimeContext;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.function.Function;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class FhirPathTestCaseBuilder {

  @Getter private final FhirPathTestBuilder parent;
  private final String description;
  private String expression;
  private Object result;

  @Nullable private String expectError = null;

  public FhirPathTestCaseBuilder expression(final String expression) {
    this.expression = expression;
    return this;
  }

  public FhirPathTestCaseBuilder expectResult(final Object result) {
    this.result = result;
    this.expectError = null;
    return this;
  }

  public FhirPathTestCaseBuilder expectError(@Nonnull final String expectError) {
    this.expectError = expectError;
    return this;
  }

  public FhirPathTestCaseBuilder expectError() {
    this.expectError = ANY_ERROR;
    return this;
  }

  public FhirPathTestCaseBuilder apply(
      @Nonnull final Function<FhirPathTestCaseBuilder, FhirPathTestCaseBuilder> function) {
    return function.apply(this);
  }

  YamlTestExecutor build(final Function<RuntimeContext, DatasetEvaluator> evaluatorFactory) {
    // Convert the result to the expected format
    final Object formattedResult;
    if (result instanceof Number || result instanceof Boolean || result instanceof String) {
      formattedResult = result;
    } else if (result == null && nonNull(expectError)) {
      formattedResult = null;
    } else if (result instanceof List && ((List<?>) result).size() == 1) {
      formattedResult = ((List<?>) result).getFirst();
    } else {
      formattedResult = result;
    }

    // Create a TestCase object
    final YamlTestDefinition.TestCase testCase =
        new YamlTestDefinition.TestCase(
            description,
            expression,
            expectError,
            formattedResult,
            null, // inputFile
            null, // model
            null, // context
            false, // disable
            null // variables
            );

    // Create and return the YamlTestExecutor
    return DefaultYamlTestExecutor.of(testCase, evaluatorFactory, java.util.Optional.empty());
  }

  @Override
  public String toString() {
    return description;
  }
}
