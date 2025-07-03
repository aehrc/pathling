package au.csiro.pathling.test.yaml.resolver;

import au.csiro.pathling.fhirpath.context.ResourceResolver;
import jakarta.annotation.Nonnull;
import java.util.Map;
import java.util.function.Function;
import lombok.Value;

@Value(staticConstructor = "of")
public class CachingResolverBuilder implements ResolverBuilder {

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
