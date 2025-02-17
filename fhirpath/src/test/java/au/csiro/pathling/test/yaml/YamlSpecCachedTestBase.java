package au.csiro.pathling.test.yaml;

import au.csiro.pathling.fhirpath.context.ResourceResolver;
import jakarta.annotation.Nonnull;
import java.util.Map;
import java.util.function.Function;
import lombok.Value;

public abstract class YamlSpecCachedTestBase extends YamlSpecTestBase {

  @Value(staticConstructor = "of")
  protected static class CachingResolverBuilder implements ResolverBuilder {

    @Nonnull
    ResolverBuilder delegate;
    @Nonnull
    Map<Function<RuntimeContext, ResourceResolver>, ResourceResolver> cache;

    @Override
    public ResourceResolver create(Function<RuntimeContext, ResourceResolver> resolveFactory) {
      return cache.computeIfAbsent(resolveFactory,
          k -> delegate.create(k));
    }
  }

  protected abstract Map<Function<RuntimeContext, ResourceResolver>, ResourceResolver> getResolverCache();

  @Override
  @Nonnull
  protected ResolverBuilder createResolverBuilder() {
    return CachingResolverBuilder.of(RuntimeContext.of(spark, fhirEncoders), getResolverCache());
  }
}
