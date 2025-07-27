package au.csiro.pathling.fhirpath.definition.def;

import au.csiro.pathling.fhirpath.definition.ChildDefinition;
import au.csiro.pathling.fhirpath.definition.ResourceDefinition;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import lombok.Value;

/**
 * The default implementation of {@link ResourceDefinition} allowing for explicit definition of its
 * children.
 */
@Value(staticConstructor = "of")
public class DefResourceDefinition implements ResourceDefinition {

  @Nonnull
  DefResourceTag resourceTag;
  @Nonnull
  List<ChildDefinition> children;
  
  @Override
  @Nonnull
  public Optional<ChildDefinition> getChildElement(@Nonnull String name) {
    return children.stream()
        .filter(child -> child.getName().equals(name))
        .findFirst();
  }

  @Nonnull
  public static DefResourceDefinition of(
      @Nonnull DefResourceTag resourceTag,
      ChildDefinition... children) {
    return DefResourceDefinition.of(resourceTag, List.of(children));
  }
}
