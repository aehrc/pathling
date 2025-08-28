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

import au.csiro.pathling.fhirpath.context.ResourceResolver;
import au.csiro.pathling.test.yaml.YamlTestBase;
import au.csiro.pathling.test.yaml.executor.YamlTestExecutor;
import au.csiro.pathling.test.yaml.resolver.ArbitraryObjectResolverFactory;
import au.csiro.pathling.test.yaml.resolver.HapiResolverFactory;
import au.csiro.pathling.test.yaml.resolver.RuntimeContext;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.junit.jupiter.api.DynamicTest;

@RequiredArgsConstructor
public class FhirPathTestBuilder {

  private final YamlTestBase testBase;
  private final Collection<FhirPathTestCaseBuilder> testCases = new ArrayList<>();

  @Nullable
  private Map<String, Object> subject = null;

  @Nullable
  IBaseResource resource = null;

  @Nullable
  private String group = null;

  @Nonnull
  public FhirPathTestBuilder withResource(@Nonnull final IBaseResource resource) {
    this.subject = null;
    this.resource = resource;
    return this;
  }

  @Nonnull
  public FhirPathTestBuilder withSubject(final Map<String, Object> subject) {
    this.subject = subject;
    this.resource = null;
    return this;
  }

  public FhirPathTestBuilder withSubject(
      @Nonnull final Function<FhirPathModelBuilder, FhirPathModelBuilder> builderFunction) {
    return withSubject(builderFunction.apply(new FhirPathModelBuilder()).getModel());
  }

  public FhirPathTestBuilder group(final String groupName) {
    this.group = groupName;
    return this;
  }

  public FhirPathTestBuilder test(final String description,
      @Nonnull final Function<FhirPathTestCaseBuilder, FhirPathTestCaseBuilder> builderFunction) {
    final String fullDescription = createTestDescription(description);
    final FhirPathTestCaseBuilder builder = new FhirPathTestCaseBuilder(this, fullDescription);
    testCases.add(builderFunction.apply(builder));
    return this;
  }

  public FhirPathTestBuilder test(final String description) {
    return test(description, builder -> builder);
  }

  /**
   * Tests that an expression equals an expected value.
   *
   * @param expected The expected value
   * @param expression The FHIRPath expression to evaluate
   * @param description The test description
   * @return This builder for method chaining
   */
  public FhirPathTestBuilder testEquals(final Object expected, final String expression,
      final String description) {
    return test(description, tc -> tc.expression(expression).expectResult(expected));
  }

  /**
   * Tests that an expression evaluates to true.
   *
   * @param expression The FHIRPath expression to evaluate
   * @param description The test description
   * @return This builder for method chaining
   */
  public FhirPathTestBuilder testTrue(final String expression, final String description) {
    return test(description, tc -> tc.expression(expression).expectResult(true));
  }

  /**
   * Tests that an expression evaluates to false.
   *
   * @param expression The FHIRPath expression to evaluate
   * @param description The test description
   * @return This builder for method chaining
   */
  public FhirPathTestBuilder testFalse(final String expression, final String description) {
    return test(description, tc -> tc.expression(expression).expectResult(false));
  }

  /**
   * Tests that an expression evaluates to an empty collection.
   *
   * @param expression The FHIRPath expression to evaluate
   * @param description The test description
   * @return This builder for method chaining
   */
  public FhirPathTestBuilder testEmpty(final String expression, final String description) {
    return test(description, tc -> tc.expression(expression).expectResult(Collections.emptyList()));
  }

  /**
   * Tests that an expression throws an error.
   *
   * @param expression The FHIRPath expression to evaluate
   * @param description The test description
   * @return This builder for method chaining
   */
  public FhirPathTestBuilder testError(final String expression, final String description) {
    return test(description, tc -> tc.expression(expression).expectError());
  }

  /**
   * Tests that an expression throws an error.
   *
   * @param errorMessage The error message to expect
   * @param expression The FHIRPath expression to evaluate
   * @param description The test description
   * @return This builder for method chaining
   */
  public FhirPathTestBuilder testError(@Nonnull final String errorMessage, final String expression,
      final String description) {
    return test(description, tc -> tc.expression(expression).expectError(errorMessage));
  }


  @Nonnull
  public Map<Object, Object> buildSubject() {
    final Map<Object, Object> result = new HashMap<>();
    result.put("resourceType", "Test");
    result.putAll(Objects.requireNonNull(subject));
    return result;
  }

  @Nonnull
  public Stream<DynamicTest> build() {

    final Function<RuntimeContext, ResourceResolver> resolverFactory;
    if (resource != null) {
      resolverFactory = HapiResolverFactory.of(resource);
    } else if (subject != null) {
      resolverFactory = ArbitraryObjectResolverFactory.of(buildSubject());
    } else {
      throw new IllegalStateException("No resource or subject provided for FhirPath tests.");
    }

    if (testCases.isEmpty()) {
      // If no test cases were added, return an empty stream
      return Stream.empty();
    }

    return testCases.stream()
        .map(tc -> {
          final YamlTestExecutor executor = tc.build(resolverFactory);
          return DynamicTest.dynamicTest(
              executor.getDescription(),
              () -> testBase.run(executor)
          );
        });
  }

  /**
   * Creates a test description by combining the current group and description. If a group is
   * present, the format will be "[group] - [description]", otherwise just the description.
   *
   * @param description The test description
   * @return The formatted test description
   */
  private String createTestDescription(final String description) {
    return group != null
           ? group + " - " + description
           : description;
  }

}
