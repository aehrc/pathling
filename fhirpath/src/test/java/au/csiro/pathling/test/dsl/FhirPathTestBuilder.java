package au.csiro.pathling.test.dsl;

import static au.csiro.pathling.test.yaml.YamlTestDefinition.TestCase.ANY_ERROR;
import static java.util.Objects.nonNull;

import au.csiro.pathling.fhirpath.context.ResourceResolver;
import au.csiro.pathling.test.yaml.FhirTypedLiteral;
import au.csiro.pathling.test.yaml.YamlSupport;
import au.csiro.pathling.test.yaml.YamlTestBase;
import au.csiro.pathling.test.yaml.YamlTestDefinition;
import au.csiro.pathling.test.yaml.executor.DefaultYamlTestExecutor;
import au.csiro.pathling.test.yaml.executor.YamlTestExecutor;
import au.csiro.pathling.test.yaml.resolver.ArbitraryObjectResolverFactory;
import au.csiro.pathling.test.yaml.resolver.HapiResolverFactory;
import au.csiro.pathling.test.yaml.resolver.RuntimeContext;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.jupiter.api.DynamicTest;

@RequiredArgsConstructor
public class FhirPathTestBuilder {

  private final YamlTestBase testBase;
  private final Collection<TestCaseBuilder> testCases = new ArrayList<>();

  @Nullable
  private Map<String, Object> subject = null;

  @Nullable
  IBaseResource resource = null;

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
      final Function<ModelBuilder, ModelBuilder> builderFunction) {
    return withSubject(builderFunction.apply(new ModelBuilder()).model);
  }

  public FhirPathTestBuilder group(final String ignoredName) {
    return this;
  }

  public FhirPathTestBuilder test(final String description,
      final Function<TestCaseBuilder, TestCaseBuilder> builderFunction) {
    final TestCaseBuilder builder = new TestCaseBuilder(this, description);
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

  @RequiredArgsConstructor
  public static class ModelBuilder {

    private final Map<String, Object> model = new HashMap<>();

    public ModelBuilder fhirType(@Nonnull final FHIRDefinedType fhirType) {
      model.put(YamlSupport.FHIR_TYPE_ANNOTATION, fhirType.toCode());
      return this;
    }

    public ModelBuilder choice(@Nonnull final String name) {
      model.put(YamlSupport.CHOICE_ANNOTATION, name);
      return this;
    }


    public ModelBuilder string(@Nonnull final String name, @Nullable final String value) {
      model.put(name, value);
      return this;
    }

    public ModelBuilder stringEmpty(@Nonnull final String name) {
      model.put(name, FhirTypedLiteral.toString(null));
      return this;
    }

    public ModelBuilder stringArray(@Nonnull final String name, final String... values) {
      model.put(name, Arrays.asList(values));
      return this;
    }

    public ModelBuilder integer(@Nonnull final String name, @Nullable final Integer value) {
      model.put(name, value);
      return this;
    }

    public ModelBuilder integerEmpty(@Nonnull final String name) {
      model.put(name, FhirTypedLiteral.toInteger(null));
      return this;
    }

    public ModelBuilder integerArray(@Nonnull final String name, final int... values) {
      final List<Integer> list = new ArrayList<>();
      for (final int value : values) {
        list.add(value);
      }
      model.put(name, list);
      return this;
    }

    public ModelBuilder decimal(@Nonnull final String name, @Nullable final Double value) {
      model.put(name, value);
      return this;
    }

    public ModelBuilder decimalEmpty(@Nonnull final String name) {
      model.put(name, FhirTypedLiteral.toDecimal(null));
      return this;
    }

    public ModelBuilder decimalArray(@Nonnull final String name, final double... values) {
      final List<Double> list = new ArrayList<>();
      for (final double value : values) {
        list.add(value);
      }
      model.put(name, list);
      return this;
    }

    public ModelBuilder bool(@Nonnull final String name, @Nullable final Boolean value) {
      model.put(name, value);
      return this;
    }

    public ModelBuilder boolEmpty(@Nonnull final String name) {
      model.put(name, FhirTypedLiteral.toBoolean(null));
      return this;
    }

    public ModelBuilder boolArray(final String name, final boolean... values) {
      final List<Boolean> list = new ArrayList<>();
      for (final boolean value : values) {
        list.add(value);
      }
      model.put(name, list);
      return this;
    }

    @Nonnull
    public ModelBuilder coding(@Nonnull final String name, @Nullable final String value) {
      model.put(name, FhirTypedLiteral.toCoding(value));
      return this;
    }

    @Nonnull
    public ModelBuilder codingEmpty(@Nonnull final String name) {
      model.put(name, FhirTypedLiteral.toCoding(null));
      return this;
    }

    @Nonnull
    public ModelBuilder codingArray(@Nonnull final String name,
        @Nonnull final String... values) {
      model.put(name, Stream.of(values)
          .map(FhirTypedLiteral::toCoding)
          .toList());
      return this;
    }
    
    public ModelBuilder element(final String name, final Consumer<ModelBuilder> builderConsumer) {
      final ModelBuilder builder = new ModelBuilder();
      builderConsumer.accept(builder);
      model.put(name, builder.model);
      return this;
    }

    public ModelBuilder elementEmpty(@Nonnull final String name) {
      model.put(name, null);
      return this;
    }

    @SafeVarargs
    public final ModelBuilder elementArray(final String name,
        final Consumer<ModelBuilder>... builders) {
      final List<Map<String, Object>> list = new ArrayList<>();
      for (final Consumer<ModelBuilder> builderConsumer : builders) {
        final ModelBuilder builder = new ModelBuilder();
        builderConsumer.accept(builder);
        list.add(builder.model);
      }
      model.put(name, list);
      return this;
    }

    public Map<String, Object> build() {
      return Collections.unmodifiableMap(model);
    }
  }

  @RequiredArgsConstructor
  public static class TestCaseBuilder {

    private final FhirPathTestBuilder parent;
    private final String description;
    private String expression;
    private Object result;
    @Nullable
    private String expectError = null;

    public TestCaseBuilder expression(final String expression) {
      this.expression = expression;
      return this;
    }

    public TestCaseBuilder expectResult(final Object result) {
      this.result = result;
      this.expectError = null;
      return this;
    }

    public TestCaseBuilder expectError(@Nonnull final String expectError) {
      this.expectError = expectError;
      return this;
    }

    public TestCaseBuilder expectError() {
      this.expectError = ANY_ERROR;
      return this;
    }

    public TestCaseBuilder apply(final Function<TestCaseBuilder, TestCaseBuilder> function) {
      return function.apply(this);
    }


    YamlTestExecutor build(
        final Function<RuntimeContext, ResourceResolver> resolverFactory) {
      // Convert the result to the expected format
      final Object formattedResult;
      if (result instanceof Number || result instanceof Boolean || result instanceof String) {
        formattedResult = result;
      } else if (result == null && nonNull(expectError)) {
        formattedResult = null;
      } else if (result instanceof List && ((List<?>) result).size() == 1) {
        formattedResult = ((List<?>) result).get(0);
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
      return DefaultYamlTestExecutor.of(
          testCase,
          resolverFactory,
          java.util.Optional.empty()
      );
    }

    @Override
    public String toString() {
      return description;
    }
  }
}
