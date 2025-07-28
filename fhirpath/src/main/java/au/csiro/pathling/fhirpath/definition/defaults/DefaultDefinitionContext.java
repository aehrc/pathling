package au.csiro.pathling.fhirpath.definition.defaults;

import au.csiro.pathling.fhirpath.definition.DefinitionContext;
import au.csiro.pathling.fhirpath.definition.ResourceDefinition;
import jakarta.annotation.Nonnull;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Value;


/**
 * A default implementation of {@link DefinitionContext} that allows for explicit definition of
 * resource types.
 */
@Value
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class DefaultDefinitionContext implements DefinitionContext {

  @Nonnull
  Map<String, ResourceDefinition> resourceDefinitions;

  @Override
  @Nonnull
  public ResourceDefinition findResourceDefinition(@Nonnull final String resourceType) {
    return Optional.ofNullable(resourceDefinitions.get(resourceType))
        .orElseThrow(
            () -> new IllegalArgumentException("Resource type not found: " + resourceType));
  }

  @Nonnull
  public static DefaultDefinitionContext of(final ResourceDefinition... resourceDefinitions) {
    return new DefaultDefinitionContext(
        Stream.of(resourceDefinitions)
            .collect(Collectors.toMap(
                ResourceDefinition::getResourceCode,
                Function.identity()
            ))
    );
  }
}
