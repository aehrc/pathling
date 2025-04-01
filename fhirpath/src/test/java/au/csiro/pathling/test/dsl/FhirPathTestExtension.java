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
        
        // Create a new builder for each test method
        FhirPathTestBuilder builder = new FhirPathTestBuilder();
        
        // Store the builder in the extension context
        Store store = context.getStore(NAMESPACE);
        store.put(testMethod.getName(), builder);
        
        // Return a single invocation context that will execute the test method with the builder
        return Stream.of(new FhirPathTestInvocationContext(builder));
    }

    private static class FhirPathTestInvocationContext implements TestTemplateInvocationContext {
        private final FhirPathTestBuilder builder;

        public FhirPathTestInvocationContext(FhirPathTestBuilder builder) {
            this.builder = builder;
        }

        @Override
        public String getDisplayName(int invocationIndex) {
            return "Configure test cases";
        }

        @Override
        public List<Extension> getAdditionalExtensions() {
            return List.of(new ParameterResolver() {
                @Override
                public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
                    return parameterContext.getParameter().getType().equals(FhirPathTestBuilder.class);
                }

                @Override
                public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
                    return builder;
                }
            }, new InvocationInterceptor() {
                @Override
                public void interceptTestTemplateMethod(Invocation<Void> invocation, ReflectiveInvocationContext<Method> invocationContext, ExtensionContext extensionContext) throws Throwable {
                    // Execute the test method to configure the builder
                    invocation.proceed();
                    
                    // After the test method has executed, get the test instance and run the tests
                    Object testInstance = extensionContext.getRequiredTestInstance();
                    if (testInstance instanceof YamlSpecTestBase testBase) {
                        List<YamlSpecTestBase.RuntimeCase> testCases = builder.buildTestCases(testBase);
                        for (YamlSpecTestBase.RuntimeCase testCase : testCases) {
                            testBase.run(testCase);
                        }
                    }
                }
            });
        }
    }
}
