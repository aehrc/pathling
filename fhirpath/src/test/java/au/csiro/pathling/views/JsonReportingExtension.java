/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

import au.csiro.pathling.views.FhirViewTest.TestParameters;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.annotation.Nonnull;
import java.io.File;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Value;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.InvocationInterceptor;
import org.junit.jupiter.api.extension.ReflectiveInvocationContext;
import org.junit.jupiter.api.extension.TestWatcher;


class JsonReportingExtension implements Extension, TestWatcher, AfterAllCallback,
    InvocationInterceptor {

  @Value
  static class TestResult {

    String testName;
    TestParameters parameters;
    String result;
  }

  @Nonnull
  private final String outputFile;

  @Nonnull
  private final List<TestResult> allResults = new ArrayList<>();

  private static final ExtensionContext.Namespace PARAMS_NAMESPACE = ExtensionContext.Namespace.create(
      JsonReportingExtension.class, "params");

  JsonReportingExtension(@Nonnull final String outputFile) {
    this.outputFile = outputFile;
  }

  @Override
  public void interceptTestTemplateMethod(final Invocation<Void> invocation,
      final ReflectiveInvocationContext<Method> invocationContext,
      final ExtensionContext extensionContext)
      throws Throwable {

    extensionContext.getStore(PARAMS_NAMESPACE)
        .put(extensionContext.getUniqueId(),
            invocationContext.getArguments().get(0));
    invocation.proceed();
  }

  private void addResult(final ExtensionContext context, final String result) {
    allResults.add(new TestResult(context.getDisplayName(),
        context.getStore(PARAMS_NAMESPACE)
            .get(context.getUniqueId(), FhirViewTest.TestParameters.class), result));
  }

  @Override
  public void testDisabled(final ExtensionContext context, final Optional<String> reason) {
    addResult(context, "disabled");
  }

  @Override
  public void testSuccessful(final ExtensionContext context) {
    addResult(context, "passed");
  }

  @Override
  public void testAborted(final ExtensionContext context, final Throwable cause) {
    addResult(context, "aborted");
  }

  @Override
  public void testFailed(final ExtensionContext context, final Throwable cause) {
    addResult(context, "failed");
  }

  @Override
  public void afterAll(final ExtensionContext context) throws Exception {

    final Map<String, List<TestResult>> resultsBySuite = allResults.stream()
        .collect(Collectors.groupingBy(tr -> tr.getParameters().getSuiteName(), LinkedHashMap::new,
            Collectors.toList()));

    final JsonNodeFactory nodeFactory = JsonNodeFactory.instance;

    final ObjectNode testReport = nodeFactory.objectNode();
    resultsBySuite.forEach((key, value) -> {
      // TODO: I think the reporting application should be changed to use the suite name as the key
      // not the file name.
      final ObjectNode suiteResults = testReport.putObject(key + ".json");
      suiteResults.putArray("tests").addAll(value.stream()
          .map(tr -> {
            final ObjectNode testResult = nodeFactory.objectNode();
            testResult.put("name", tr.getParameters().getTestName());
            testResult.putObject("result")
                .put("passed", "passed".equals(tr.getResult()));
            return testResult;
          }).collect(Collectors.toList()));

    });
    System.out.printf("Writing JSON report to to %s\n", outputFile);
    final ObjectMapper mapper = new ObjectMapper();
    mapper.writerWithDefaultPrettyPrinter().writeValue(new File(outputFile), testReport);
  }
}
