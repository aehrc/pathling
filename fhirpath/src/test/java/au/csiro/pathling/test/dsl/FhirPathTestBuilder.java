package au.csiro.pathling.test.dsl;

import au.csiro.pathling.fhirpath.context.ResourceResolver;
import au.csiro.pathling.test.TestResources;
import au.csiro.pathling.test.yaml.YamlSpecTestBase;
import au.csiro.pathling.test.yaml.YamlSupport;
import jakarta.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.Value;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;

@RequiredArgsConstructor
public class FhirPathTestBuilder {
    private final Map<String, Object> subject = new HashMap<>();
    private final List<TestCaseBuilder> testCases = new ArrayList<>();
    private String currentGroup;

    public SubjectBuilder withSubject() {
        return new SubjectBuilder(this);
    }

    public FhirPathTestBuilder group(String name) {
        this.currentGroup = name;
        return this;
    }

    public TestCaseBuilder test(String description) {
        TestCaseBuilder builder = new TestCaseBuilder(this, description);
        testCases.add(builder);
        return builder;
    }

    @Nonnull
    public Map<Object, Object> buildSubject() {
        Map<Object, Object> result = new HashMap<>();
        result.put("resourceType", "Test");
        result.putAll(subject);
        return result;
    }

    @Nonnull
    public List<YamlSpecTestBase.RuntimeCase> buildTestCases(YamlSpecTestBase testBase) {
        Map<Object, Object> subjectOM = buildSubject();
        return testCases.stream()
                .map(tc -> tc.build(subjectOM, testBase))
                .toList();
    }

    void addToSubject(String key, Object value) {
        subject.put(key, value);
    }

    @RequiredArgsConstructor
    public static class SubjectBuilder {
        private final FhirPathTestBuilder parent;

        public SubjectBuilder string(String name, String value) {
            parent.addToSubject(name, value);
            return this;
        }

        public SubjectBuilder stringArray(String name, String... values) {
            parent.addToSubject(name, Arrays.asList(values));
            return this;
        }

        public SubjectBuilder integer(String name, int value) {
            parent.addToSubject(name, value);
            return this;
        }

        public SubjectBuilder integerArray(String name, int... values) {
            List<Integer> list = new ArrayList<>();
            for (int value : values) {
                list.add(value);
            }
            parent.addToSubject(name, list);
            return this;
        }

        public SubjectBuilder decimal(String name, double value) {
            parent.addToSubject(name, value);
            return this;
        }

        public SubjectBuilder decimalArray(String name, double... values) {
            List<Double> list = new ArrayList<>();
            for (double value : values) {
                list.add(value);
            }
            parent.addToSubject(name, list);
            return this;
        }

        public SubjectBuilder bool(String name, boolean value) {
            parent.addToSubject(name, value);
            return this;
        }

        public SubjectBuilder boolArray(String name, boolean... values) {
            List<Boolean> list = new ArrayList<>();
            for (boolean value : values) {
                list.add(value);
            }
            parent.addToSubject(name, list);
            return this;
        }

        public SubjectBuilder dateTime(String name, String value) {
            parent.addToSubject(name, YamlSupport.FhirTypedLiteral.of(
                org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType.DATETIME, value));
            return this;
        }

        public SubjectBuilder dateTimeArray(String name, String... values) {
            List<YamlSupport.FhirTypedLiteral> list = new ArrayList<>();
            for (String value : values) {
                list.add(YamlSupport.FhirTypedLiteral.of(
                    org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType.DATETIME, value));
            }
            parent.addToSubject(name, list);
            return this;
        }

        public SubjectBuilder date(String name, String value) {
            parent.addToSubject(name, YamlSupport.FhirTypedLiteral.of(
                org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType.DATE, value));
            return this;
        }

        public SubjectBuilder time(String name, String value) {
            parent.addToSubject(name, YamlSupport.FhirTypedLiteral.of(
                org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType.TIME, value));
            return this;
        }

        public SubjectBuilder coding(String name, String value) {
            parent.addToSubject(name, YamlSupport.FhirTypedLiteral.of(
                org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType.CODING, value));
            return this;
        }

        public SubjectBuilder codingArray(String name, String... values) {
            List<YamlSupport.FhirTypedLiteral> list = new ArrayList<>();
            for (String value : values) {
                list.add(YamlSupport.FhirTypedLiteral.of(
                    org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType.CODING, value));
            }
            parent.addToSubject(name, list);
            return this;
        }

        public SubjectBuilder complex(String name, Consumer<ComplexBuilder> builderConsumer) {
            ComplexBuilder builder = new ComplexBuilder();
            builderConsumer.accept(builder);
            parent.addToSubject(name, builder.build());
            return this;
        }

        public SubjectBuilder complexArray(String name, Consumer<ComplexBuilder>... builders) {
            List<Map<String, Object>> list = new ArrayList<>();
            for (Consumer<ComplexBuilder> builderConsumer : builders) {
                ComplexBuilder builder = new ComplexBuilder();
                builderConsumer.accept(builder);
                list.add(builder.build());
            }
            parent.addToSubject(name, list);
            return this;
        }

        public FhirPathTestBuilder and() {
            return parent;
        }
    }

    public static class ComplexBuilder {
        private final Map<String, Object> properties = new HashMap<>();

        public ComplexBuilder property(String name, Object value) {
            properties.put(name, value);
            return this;
        }

        public ComplexBuilder complex(String name, Consumer<ComplexBuilder> builderConsumer) {
            ComplexBuilder builder = new ComplexBuilder();
            builderConsumer.accept(builder);
            properties.put(name, builder.build());
            return this;
        }

        public ComplexBuilder complexArray(String name, Consumer<ComplexBuilder>... builders) {
            List<Map<String, Object>> list = new ArrayList<>();
            for (Consumer<ComplexBuilder> builderConsumer : builders) {
                ComplexBuilder builder = new ComplexBuilder();
                builderConsumer.accept(builder);
                list.add(builder.build());
            }
            properties.put(name, list);
            return this;
        }

        Map<String, Object> build() {
            return properties;
        }
    }

    @RequiredArgsConstructor
    public static class TestCaseBuilder {
        private final FhirPathTestBuilder parent;
        private final String description;
        private String expression;
        private Object result;
        private boolean expectError = false;
        private String inputFile;

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

        public TestCaseBuilder inputFile(String inputFile) {
            this.inputFile = inputFile;
            return this;
        }

        public FhirPathTestBuilder and() {
            return parent;
        }

        YamlSpecTestBase.RuntimeCase build(Map<Object, Object> subjectOM, YamlSpecTestBase testBase) {
            // Convert the result to the expected format
            Object formattedResult;
            if (result instanceof Number || result instanceof Boolean || result instanceof String) {
                formattedResult = List.of(result);
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
                    inputFile,
                    null,
                    null,
                    false
                );

            // Create the resolver factory
            Function<YamlSpecTestBase.RuntimeContext, ResourceResolver> resolverFactory;
            if (inputFile != null) {
                resolverFactory = YamlSpecTestBase.FhirResolverFactory.of(
                    TestResources.getResourceAsString(
                        ((FhirPathDslTestBase)testBase).getResourceBasePath() + "/" + inputFile));
            } else {
                resolverFactory = YamlSpecTestBase.OMResolverFactory.of(subjectOM);
            }

            // Create and return the RuntimeCase
            return YamlSpecTestBase.StdRuntimeCase.of(
                testCase,
                resolverFactory,
                java.util.Optional.empty()
            );
        }
    }
}
