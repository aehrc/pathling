package au.csiro.pathling.fhirpath.variable;

import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A chain of variable resolvers that can be queried in sequence.
 *
 * @author John Grimes
 */
public record VariableResolverChain(@Nonnull List<EnvironmentVariableResolver> resolvers) implements
    EnvironmentVariableResolver {

  @Override
  public Optional<Collection> get(@Nonnull final String name) {
    //noinspection OptionalGetWithoutIsPresent
    return resolvers.stream()
        .map(resolver -> resolver.get(name))
        .filter(Optional::isPresent)
        .findFirst()
        .map(Optional::get);
  }

  /**
   * Create a new resolver chain with the default set of resolvers.
   *
   * @param resource A collection representing the resource being queried
   * @param inputContext The input context for the query
   * @return A new resolver chain
   */
  @Nonnull
  public static VariableResolverChain withDefaults(@Nonnull final ResourceCollection resource,
      @Nonnull final Collection inputContext) {
    final List<EnvironmentVariableResolver> resolvers = new ArrayList<>();
    resolvers.add(new BuiltInConstantResolver());
    resolvers.add(new ContextVariableResolver(resource, inputContext));
    resolvers.add(new Hl7ValueSetResolver());
    resolvers.add(new Hl7ExtensionResolver());
    resolvers.add(new UnsupportedVariableResolver());
    return new VariableResolverChain(resolvers);
  }

  /**
   * Create a new resolver chain with the default set of resolvers, and additional variables.
   *
   * @param resource A collection representing the resource being queried
   * @param inputContext The input context for the query
   * @param additionalVariables A map of additional variables to add to the chain
   * @return A new resolver chain
   */
  @Nonnull
  public static VariableResolverChain withDefaults(@Nonnull final ResourceCollection resource,
      @Nonnull final Collection inputContext,
      @Nonnull final Map<String, Collection> additionalVariables) {
    final VariableResolverChain chain = withDefaults(resource, inputContext);
    chain.resolvers().add(new SuppliedVariableResolver(additionalVariables));
    return chain;
  }

}
