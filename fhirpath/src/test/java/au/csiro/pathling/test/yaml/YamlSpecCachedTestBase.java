package au.csiro.pathling.test.yaml;

import au.csiro.pathling.fhirpath.context.ResourceResolver;
import jakarta.annotation.Nonnull;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import lombok.Value;

public abstract class YamlSpecCachedTestBase extends YamlSpecTestBase {

  @Nonnull
  private static final Map<Class<? extends YamlSpecCachedTestBase>,
      Map<Function<RuntimeContext, ResourceResolver>, ResourceResolver>> CACHE =
      Collections.synchronizedMap(new HashMap<>());


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

  protected Map<Function<RuntimeContext, ResourceResolver>, ResourceResolver> getResolverCache() {
    return CACHE.computeIfAbsent(getClass(), __ -> new HashMap<>());
  }

  @Override
  @Nonnull
  protected ResolverBuilder createResolverBuilder() {
    return CachingResolverBuilder.of(RuntimeContext.of(spark, fhirEncoders), getResolverCache());
  }
}
