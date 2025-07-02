package au.csiro.pathling.test.yaml;

import au.csiro.pathling.fhirpath.context.ResourceResolver;
import au.csiro.pathling.test.yaml.resolver.CachingResolverBuilder;
import au.csiro.pathling.test.yaml.resolver.ResolverBuilder;
import jakarta.annotation.Nonnull;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public abstract class YamlCachedTestBase extends YamlTestBase {

  @Nonnull
  private static final Map<Class<? extends YamlCachedTestBase>,
      Map<Function<RuntimeContext, ResourceResolver>, ResourceResolver>> CACHE =
      Collections.synchronizedMap(new HashMap<>());


  protected Map<Function<RuntimeContext, ResourceResolver>, ResourceResolver> getResolverCache() {
    return CACHE.computeIfAbsent(getClass(), __ -> new HashMap<>());
  }

  @Override
  @Nonnull
  protected ResolverBuilder createResolverBuilder() {
    return CachingResolverBuilder.of(RuntimeContext.of(spark, fhirEncoders), getResolverCache());
  }
}
