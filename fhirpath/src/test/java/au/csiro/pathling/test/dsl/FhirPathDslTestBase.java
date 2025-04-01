package au.csiro.pathling.test.dsl;

import au.csiro.pathling.fhirpath.context.ResourceResolver;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.yaml.YamlSpecTestBase;
import jakarta.annotation.Nonnull;
import lombok.Value;
import org.junit.jupiter.params.ParameterizedTest;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

@SpringBootUnitTest
public abstract class FhirPathDslTestBase extends YamlSpecTestBase {

    private static final Map<Function<RuntimeContext, ResourceResolver>, ResourceResolver> CACHE =
            Collections.synchronizedMap(new HashMap<>());

    @Nonnull
    protected String getResourceBasePath() {
        return "fhirpath-ptl/resources";
    }

    // This method is used by the parameterized test framework
    @ParameterizedTest
    @FhirPathTest
    void runTest(YamlSpecTestBase.RuntimeCase testCase) {
        run(testCase);
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
