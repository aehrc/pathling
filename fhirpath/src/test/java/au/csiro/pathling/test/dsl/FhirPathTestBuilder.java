package au.csiro.pathling.test.dsl;

import au.csiro.pathling.fhirpath.context.ResourceResolver;
import au.csiro.pathling.test.yaml.FhirTypedLiteral;
import au.csiro.pathling.test.yaml.YamlSpecTestBase;
import au.csiro.pathling.test.yaml.YamlSupport;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.jupiter.api.DynamicTest;

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

    public ModelBuilder fhirType(@Nonnull final FHIRDefinedType fhirType) {
      model.put(YamlSupport.FHIR_TYPE_ANNOTATION, fhirType.toCode());
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

    public ModelBuilder stringArray(@Nonnull final String name, String... values) {
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

    public ModelBuilder integerArray(@Nonnull final String name, int... values) {
      List<Integer> list = new ArrayList<>();
      for (int value : values) {
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

    public ModelBuilder decimalArray(@Nonnull final String name, double... values) {
      List<Double> list = new ArrayList<>();
      for (double value : values) {
        list.add(value);
      }
      model.put(name, list);
      return this;
    }

    public ModelBuilder bool(@Nonnull final String name, @Nullable Boolean value) {
      model.put(name, value);
      return this;
    }

    public ModelBuilder boolEmpty(@Nonnull final String name) {
      model.put(name, FhirTypedLiteral.toBoolean(null));
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

    @Nonnull
    public ModelBuilder dateTime(@Nonnull final String name, @Nullable final String value) {
      model.put(name, FhirTypedLiteral.toDateTime(value));
      return this;
    }

    @Nonnull
    public ModelBuilder dateTimeEmpty(@Nonnull final String name) {
      model.put(name, FhirTypedLiteral.toDateTime(null));
      return this;
    }

    @Nonnull
    public ModelBuilder dateTimeArray(@Nonnull final String name,
        @Nonnull final String... values) {
      model.put(name, Stream.of(values)
          .map(FhirTypedLiteral::toDateTime)
          .toList());
      return this;
    }

    @Nonnull
    public ModelBuilder date(@Nonnull final String name, @Nullable final String value) {
      model.put(name, FhirTypedLiteral.toDate(value));
      return this;
    }

    @Nonnull
    public ModelBuilder dateEmpty(@Nonnull final String name) {
      model.put(name, FhirTypedLiteral.toDate(null));
      return this;
    }

    @Nonnull
    public ModelBuilder dateArray(@Nonnull final String name,
        @Nonnull final String... values) {
      model.put(name, Stream.of(values)
          .map(FhirTypedLiteral::toDate)
          .toList());
      return this;
    }

    @Nonnull
    public ModelBuilder time(@Nonnull final String name, @Nullable final String value) {
      model.put(name, FhirTypedLiteral.toTime(value));
      return this;
    }

    @Nonnull
    public ModelBuilder timeEmpty(@Nonnull final String name) {
      model.put(name, FhirTypedLiteral.toTime(null));
      return this;
    }

    @Nonnull
    public ModelBuilder timeArray(@Nonnull final String name,
        @Nonnull final String... values) {
      model.put(name, Stream.of(values)
          .map(FhirTypedLiteral::toTime)
          .toList());
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

    @Nonnull
    public ModelBuilder quantity(@Nonnull final String name, @Nullable final String literalValue) {
      model.put(name, FhirTypedLiteral.toQuantity(literalValue));
      return this;
    }

    @Nonnull
    public ModelBuilder quantityEmpty(@Nonnull final String name) {
      model.put(name, FhirTypedLiteral.toQuantity(null));
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


    public ModelBuilder element(String name, Consumer<ModelBuilder> builderConsumer) {
      ModelBuilder builder = new ModelBuilder();
      builderConsumer.accept(builder);
      model.put(name, builder.model);
      return this;
    }

    public ModelBuilder elementEmpty(@Nonnull final String name) {
      model.put(name, null);
      return this;
    }

    @SafeVarargs
    public final ModelBuilder elementArray(String name, Consumer<ModelBuilder>... builders) {
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
