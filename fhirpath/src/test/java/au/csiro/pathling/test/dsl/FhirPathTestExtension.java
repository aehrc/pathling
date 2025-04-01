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
        
        // Execute the test method to configure the builder
        try {
            Object testInstance = context.getRequiredTestInstance();
            testMethod.invoke(testInstance, builder);
            
            // Get the test cases from the builder
            if (testInstance instanceof YamlSpecTestBase testBase) {
                List<YamlSpecTestBase.RuntimeCase> testCases = builder.buildTestCases(testBase);
                // Return each test case as a separate argument
                return testCases.stream().map(Arguments::of);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to execute test method", e);
        }
        
        return Stream.empty();
    }
}
