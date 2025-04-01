package au.csiro.pathling.test.dsl;

import au.csiro.pathling.fhirpath.context.ResourceResolver;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.yaml.YamlSpecTestBase;
import jakarta.annotation.Nonnull;
import lombok.Value;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.lang.reflect.Method;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;

@SpringBootUnitTest
public abstract class FhirPathDslTestBase extends YamlSpecTestBase {

    private static final Map<Function<RuntimeContext, ResourceResolver>, ResourceResolver> CACHE =
            Collections.synchronizedMap(new HashMap<>());

    @Nonnull
    protected String getResourceBasePath() {
        return "fhirpath-ptl/resources";
    }

    @TestFactory
    public Stream<DynamicTest> generateTests() {
        List<DynamicTest> allTests = new ArrayList<>();
        
        // Find all methods annotated with @FhirPathTest
        for (Method method : getClass().getDeclaredMethods()) {
            if (method.isAnnotationPresent(FhirPathTest.class)) {
                // Create a builder for this test method
                FhirPathTestBuilder builder = new FhirPathTestBuilder();
                
                try {
                    // Invoke the test method to configure the builder
                    method.invoke(this, builder);
                    
                    // Build test cases
                    List<YamlSpecTestBase.RuntimeCase> testCases = builder.buildTestCases(this);
                    
                    // Create a dynamic test for each test case
                    for (YamlSpecTestBase.RuntimeCase testCase : testCases) {
                        String displayName = testCase.toString();
                        allTests.add(DynamicTest.dynamicTest(displayName, () -> run(testCase)));
                    }
                } catch (Exception e) {
                    throw new RuntimeException("Failed to generate tests for method: " + method.getName(), e);
                }
            }
        }
        
        return allTests.stream();
    }

    @Nonnull
    @Override
    protected ResolverBuilder createResolverBuilder() {
        return CachingResolverBuilder.of(RuntimeContext.of(spark, fhirEncoders), CACHE);
    }

    @Value(staticConstructor = "of")
    protected static class CachingResolverBuilder implements ResolverBuilder {

        @Nonnull
        ResolverBuilder delegate;
        @Nonnull
        Map<Function<RuntimeContext, ResourceResolver>, ResourceResolver> cache;

        @Override
        @Nonnull
        public ResourceResolver create(
                @Nonnull final Function<RuntimeContext, ResourceResolver> resolveFactory) {
            return cache.computeIfAbsent(resolveFactory, delegate::create);
        }
    }
}
