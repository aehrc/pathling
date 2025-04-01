package au.csiro.pathling.test.dsl;

import au.csiro.pathling.test.yaml.YamlSpecTestBase;
import org.junit.jupiter.api.extension.*;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

import java.lang.reflect.Method;
import java.util.List;
import java.util.stream.Stream;

public class FhirPathTestExtension implements ArgumentsProvider {

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
        Method testMethod = context.getRequiredTestMethod();
        
        // Create a new builder for the test method
        FhirPathTestBuilder builder = new FhirPathTestBuilder();
        
        try {
            // Get the test instance if available
            Object testInstance = context.getTestInstance().orElse(null);
            if (testInstance == null) {
                // If test instance is not available, create a new instance
                Class<?> testClass = context.getRequiredTestClass();
                testInstance = testClass.getDeclaredConstructor().newInstance();
            }
            
            // Execute the test method to configure the builder
            testMethod.invoke(testInstance, builder);
            
            // Get the test cases from the builder
            if (testInstance instanceof YamlSpecTestBase testBase) {
                List<YamlSpecTestBase.RuntimeCase> testCases = builder.buildTestCases(testBase);
                // Return each test case as a separate argument
                return testCases.stream().map(Arguments::of);
            } else {
                // For non-YamlSpecTestBase test classes, we need a different approach
                // Create a dummy YamlSpecTestBase to build test cases
                YamlSpecTestBase dummyBase = new YamlSpecTestBase() {};
                List<YamlSpecTestBase.RuntimeCase> testCases = builder.buildTestCases(dummyBase);
                return testCases.stream().map(Arguments::of);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to execute test method: " + e.getMessage(), e);
        }
    }
}
