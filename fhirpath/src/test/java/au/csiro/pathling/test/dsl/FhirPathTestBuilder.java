package au.csiro.pathling.test.dsl;

import au.csiro.pathling.fhirpath.context.ResourceResolver;
import au.csiro.pathling.test.yaml.YamlSpecTestBase;
import au.csiro.pathling.test.yaml.YamlSupport;
import au.csiro.pathling.test.yaml.YamlSupport.FhirTypedLiteral;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.jupiter.api.DynamicTest;
import jakarta.annotation.Nonnull;
import lombok.RequiredArgsConstructor;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

@RequiredArgsConstructor
public class FhirPathTestBuilder {

  private final YamlSpecTestBase testBase;
  private final List<TestCaseBuilder> testCases = new ArrayList<>();
  private Map<String, Object> subject = new HashMap<>();
  private String currentGroup;

  public FhirPathTestBuilder withSubject(Map<String, Object> subject) {
    this.subject = subject;
    return this;
  }

  public FhirPathTestBuilder withSubject(Function<ModelBuilder, ModelBuilder> builderFunction) {
    return withSubject(builderFunction.apply(new ModelBuilder()).model);
  }

  public FhirPathTestBuilder group(String name) {
    this.currentGroup = name;
    return this;
  }

  public FhirPathTestBuilder test(String description,
      Function<TestCaseBuilder, TestCaseBuilder> builderFunction) {
    TestCaseBuilder builder = new TestCaseBuilder(this, description);
    testCases.add(builderFunction.apply(builder));
    return this;
  }

  public FhirPathTestBuilder test(String description) {
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
  public FhirPathTestBuilder testEquals(Object expected, String expression, String description) {
    return test(description, tc -> tc.expression(expression).expectResult(expected));
  }

  /**
   * Tests that an expression evaluates to true.
   *
   * @param expression The FHIRPath expression to evaluate
   * @param description The test description
   * @return This builder for method chaining
   */
  public FhirPathTestBuilder testTrue(String expression, String description) {
    return test(description, tc -> tc.expression(expression).expectResult(true));
  }

  /**
   * Tests that an expression evaluates to false.
   *
   * @param expression The FHIRPath expression to evaluate
   * @param description The test description
   * @return This builder for method chaining
   */
  public FhirPathTestBuilder testFalse(String expression, String description) {
    return test(description, tc -> tc.expression(expression).expectResult(false));
  }

  /**
   * Tests that an expression evaluates to an empty collection.
   *
   * @param expression The FHIRPath expression to evaluate
   * @param description The test description
   * @return This builder for method chaining
   */
  public FhirPathTestBuilder testEmpty(String expression, String description) {
    return test(description, tc -> tc.expression(expression).expectResult(Collections.emptyList()));
  }

  /**
   * Tests that an expression throws an error.
   *
   * @param expression The FHIRPath expression to evaluate
   * @param description The test description
   * @return This builder for method chaining
   */
  public FhirPathTestBuilder testError(String expression, String description) {
    return test(description, tc -> tc.expression(expression).expectError());
  }

  @Nonnull
  public Map<Object, Object> buildSubject() {
    Map<Object, Object> result = new HashMap<>();
    result.put("resourceType", "Test");
    result.putAll(subject);
    return result;
  }

  @Nonnull
  public Stream<DynamicTest> build() {
    Map<Object, Object> subjectOM = buildSubject();
    Function<YamlSpecTestBase.RuntimeContext, ResourceResolver> resolverFactory =
        YamlSpecTestBase.OMResolverFactory.of(subjectOM);

    if (testCases.isEmpty()) {
      // If no test cases were added, return an empty stream
      return Stream.empty();
    }

    return testCases.stream()
        .map(tc -> {
          YamlSpecTestBase.RuntimeCase runtimeCase = tc.build(resolverFactory);
          return DynamicTest.dynamicTest(
              runtimeCase.getDescription(),
              () -> testBase.run(runtimeCase)
          );
        });
  }

  @RequiredArgsConstructor
  public static class ModelBuilder {

    private final Map<String, Object> model = new HashMap<>();

    public ModelBuilder string(String name, String value) {
      model.put(name, value);
      return this;
    }

    public ModelBuilder stringArray(String name, String... values) {
      model.put(name, Arrays.asList(values));
      return this;
    }

    public ModelBuilder integer(String name, int value) {
      model.put(name, value);
      return this;
    }

    public ModelBuilder integerArray(String name, int... values) {
      List<Integer> list = new ArrayList<>();
      for (int value : values) {
        list.add(value);
      }
      model.put(name, list);
      return this;
    }

    public ModelBuilder decimal(String name, double value) {
      model.put(name, value);
      return this;
    }

    public ModelBuilder decimalArray(String name, double... values) {
      List<Double> list = new ArrayList<>();
      for (double value : values) {
        list.add(value);
      }
      model.put(name, list);
      return this;
    }

    public ModelBuilder bool(String name, boolean value) {
      model.put(name, value);
      return this;
    }

    public ModelBuilder boolArray(String name, boolean... values) {
      List<Boolean> list = new ArrayList<>();
      for (boolean value : values) {
        list.add(value);
      }
      model.put(name, list);
      return this;
    }

    public ModelBuilder dateTime(String name, String value) {
      model.put(name, YamlSupport.FhirTypedLiteral.of(
          FHIRDefinedType.DATETIME, value));
      return this;
    }

    public ModelBuilder dateTimeArray(String name, String... values) {
      List<YamlSupport.FhirTypedLiteral> list = new ArrayList<>();
      for (String value : values) {
        list.add(YamlSupport.FhirTypedLiteral.of(
            FHIRDefinedType.DATETIME, value));
      }
      model.put(name, list);
      return this;
    }

    public ModelBuilder date(String name, String value) {
      model.put(name, YamlSupport.FhirTypedLiteral.of(
          FHIRDefinedType.DATE, value));
      return this;
    }

    public ModelBuilder time(String name, String value) {
      model.put(name, YamlSupport.FhirTypedLiteral.of(
          FHIRDefinedType.TIME, value));
      return this;
    }

    public ModelBuilder coding(String name, String value) {
      model.put(name, YamlSupport.FhirTypedLiteral.of(
          FHIRDefinedType.CODING, value));
      return this;
    }

    public ModelBuilder codingArray(String name, String... values) {
      List<YamlSupport.FhirTypedLiteral> list = new ArrayList<>();
      for (String value : values) {
        list.add(YamlSupport.FhirTypedLiteral.of(
            FHIRDefinedType.CODING, value));
      }
      model.put(name, list);
      return this;
    }

    public ModelBuilder quantity(String name, String literalValue) {
      model.put(name, YamlSupport.FhirTypedLiteral.of(
          FHIRDefinedType.QUANTITY, literalValue));
      return this;
    }

    @Nonnull
    public ModelBuilder quantityArray(@Nonnull final String name,
        @Nonnull final String... literalValues) {
      model.put(name, Stream.of(literalValues)
          .map(FhirTypedLiteral::toQuantity)
          .toList());
      return this;
    }


    public ModelBuilder complex(String name, Consumer<ModelBuilder> builderConsumer) {
      ModelBuilder builder = new ModelBuilder();
      builderConsumer.accept(builder);
      model.put(name, builder.model);
      return this;
    }

    public ModelBuilder complexArray(String name, Consumer<ModelBuilder>... builders) {
      List<Map<String, Object>> list = new ArrayList<>();
      for (Consumer<ModelBuilder> builderConsumer : builders) {
        ModelBuilder builder = new ModelBuilder();
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
    private boolean expectError = false;

    public TestCaseBuilder expression(String expression) {
      this.expression = expression;
      return this;
    }

    public TestCaseBuilder expectResult(Object result) {
      this.result = result;
      return this;
    }

    public TestCaseBuilder expectError() {
      this.expectError = true;
      return this;
    }

    public TestCaseBuilder apply(Function<TestCaseBuilder, TestCaseBuilder> function) {
      return function.apply(this);
    }


    YamlSpecTestBase.RuntimeCase build(
        Function<YamlSpecTestBase.RuntimeContext, ResourceResolver> resolverFactory) {
      // Convert the result to the expected format
      Object formattedResult;
      if (result instanceof Number || result instanceof Boolean || result instanceof String) {
        formattedResult = result;
      } else if (result == null && expectError) {
        formattedResult = null;
      } else if (result instanceof List && ((List<?>) result).size() == 1) {
        formattedResult = ((List<?>) result).get(0);
      } else {
        formattedResult = result;
      }

      // Create a TestCase object
      au.csiro.pathling.test.yaml.FhipathTestSpec.TestCase testCase =
          new au.csiro.pathling.test.yaml.FhipathTestSpec.TestCase(
              description,
              expression,
              expectError,
              formattedResult,
              null, // inputFile
              null, // model
              null, // context
              false // disable
          );

      // Create and return the RuntimeCase
      return YamlSpecTestBase.StdRuntimeCase.of(
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
