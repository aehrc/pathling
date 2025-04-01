package au.csiro.pathling.test.dsl;

import au.csiro.pathling.test.yaml.YamlSpecTestBase;
import org.junit.jupiter.api.extension.*;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ExtensionContext.Store;

import java.lang.reflect.Method;
import java.util.List;
import java.util.stream.Stream;

public class FhirPathTestExtension implements TestTemplateInvocationContextProvider {

    private static final Namespace NAMESPACE = Namespace.create(FhirPathTestExtension.class);

    @Override
    public boolean supportsTestTemplate(ExtensionContext context) {
        return context.getTestMethod()
                .map(method -> method.isAnnotationPresent(FhirPathTest.class))
                .orElse(false);
    }

    @Override
    public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(ExtensionContext context) {
        Method testMethod = context.getRequiredTestMethod();
        Object testInstance = context.getRequiredTestInstance();
        
        // Get or create the test builder
        Store store = context.getStore(NAMESPACE);
        FhirPathTestBuilder builder = store.getOrComputeIfAbsent(
                testMethod.getName(), 
                key -> {
                    try {
                        FhirPathTestBuilder newBuilder = new FhirPathTestBuilder();
                        testMethod.invoke(testInstance, newBuilder);
                        return newBuilder;
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to invoke test method", e);
                    }
                },
                FhirPathTestBuilder.class);
        
        // Get the YamlSpecTestBase instance
        YamlSpecTestBase testBase = (YamlSpecTestBase) testInstance;
        
        // Build the test cases
        List<YamlSpecTestBase.RuntimeCase> testCases = builder.buildTestCases(testBase);
        
        // Create invocation contexts for each test case
        return testCases.stream()
                .map(testCase -> new FhirPathTestInvocationContext(testCase, testBase));
    }

    private static class FhirPathTestInvocationContext implements TestTemplateInvocationContext {
        private final YamlSpecTestBase.RuntimeCase testCase;
        private final YamlSpecTestBase testBase;

        public FhirPathTestInvocationContext(YamlSpecTestBase.RuntimeCase testCase, YamlSpecTestBase testBase) {
            this.testCase = testCase;
            this.testBase = testBase;
        }

        @Override
        public String getDisplayName(int invocationIndex) {
            return testCase.toString();
        }

        @Override
        public List<Extension> getAdditionalExtensions() {
            return List.of(new ParameterResolver() {
                @Override
                public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
                    return parameterContext.getParameter().getType().equals(YamlSpecTestBase.RuntimeCase.class);
                }

                @Override
                public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
                    return testCase;
                }
            });
        }
    }
}
