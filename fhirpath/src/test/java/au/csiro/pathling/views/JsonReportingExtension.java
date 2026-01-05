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

package au.csiro.pathling.views;

import static java.util.Objects.requireNonNull;

import au.csiro.pathling.views.FhirViewTest.TestParameters;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.File;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.InvocationInterceptor;
import org.junit.jupiter.api.extension.ReflectiveInvocationContext;
import org.junit.jupiter.api.extension.TestWatcher;

@Slf4j
class JsonReportingExtension
    implements Extension, TestWatcher, AfterAllCallback, InvocationInterceptor {

  record TestResult(String testName, TestParameters parameters, String result) {}

  @Nonnull private final String outputFile;

  @Nonnull private final List<TestResult> allResults = new ArrayList<>();

  private static final ExtensionContext.Namespace PARAMS_NAMESPACE =
      ExtensionContext.Namespace.create(JsonReportingExtension.class, "params");

  JsonReportingExtension(@Nonnull final String outputFile) {
    this.outputFile = outputFile;
  }

  @Override
  public void interceptTestTemplateMethod(
      @Nullable final Invocation<Void> invocation,
      @Nullable final ReflectiveInvocationContext<Method> invocationContext,
      @Nullable final ExtensionContext extensionContext)
      throws Throwable {

    requireNonNull(extensionContext)
        .getStore(PARAMS_NAMESPACE)
        .put(
            extensionContext.getUniqueId(),
            requireNonNull(invocationContext).getArguments().get(0));
    requireNonNull(invocation).proceed();
  }

  private void addResult(@Nullable final ExtensionContext context, @Nullable final String result) {
    allResults.add(
        new TestResult(
            requireNonNull(context).getDisplayName(),
            context
                .getStore(PARAMS_NAMESPACE)
                .get(context.getUniqueId(), FhirViewTest.TestParameters.class),
            requireNonNull(result)));
  }

  @Override
  public void testDisabled(
      @Nullable final ExtensionContext context, @Nullable final Optional<String> reason) {
    addResult(context, "disabled");
  }

  @Override
  public void testSuccessful(@Nullable final ExtensionContext context) {
    addResult(context, "passed");
  }

  @Override
  public void testAborted(
      @Nullable final ExtensionContext context, @Nullable final Throwable cause) {
    addResult(context, "aborted");
  }

  @Override
  public void testFailed(
      @Nullable final ExtensionContext context, @Nullable final Throwable cause) {
    addResult(context, "failed");
  }

  @Override
  public void afterAll(@Nullable final ExtensionContext context) throws Exception {

    final Map<String, List<TestResult>> resultsBySuite =
        allResults.stream()
            .collect(
                Collectors.groupingBy(
                    tr -> tr.parameters().suiteName(), LinkedHashMap::new, Collectors.toList()));

    final JsonNodeFactory nodeFactory = JsonNodeFactory.instance;

    final ObjectNode testReport = nodeFactory.objectNode();
    resultsBySuite.forEach(
        (key, value) -> {
          final ObjectNode suiteResults = testReport.putObject(key + ".json");
          suiteResults
              .putArray("tests")
              .addAll(
                  value.stream()
                      .map(
                          tr -> {
                            final ObjectNode testResult = nodeFactory.objectNode();
                            testResult.set(
                                "name", nodeFactory.textNode(tr.parameters().testName()));
                            testResult
                                .putObject("result")
                                .set(
                                    "passed",
                                    nodeFactory.booleanNode("passed".equals(tr.result())));
                            return testResult;
                          })
                      .toList());
        });
    log.info("Writing JSON report to {}", outputFile);
    final ObjectMapper mapper = new ObjectMapper();
    mapper.writerWithDefaultPrettyPrinter().writeValue(new File(outputFile), testReport);
  }
}
