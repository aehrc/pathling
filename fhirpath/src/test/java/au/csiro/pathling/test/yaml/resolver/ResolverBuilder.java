package au.csiro.pathling.test.yaml.resolver;

import au.csiro.pathling.fhirpath.context.ResourceResolver;
import au.csiro.pathling.test.yaml.RuntimeContext;
import jakarta.annotation.Nonnull;
import java.util.function.Function;

/**
 * Interface for building resource resolvers with specific context.
 */
@FunctionalInterface
public interface ResolverBuilder {

  @Nonnull
  ResourceResolver create(
      @Nonnull final Function<RuntimeContext, ResourceResolver> resolveFactory);
}
