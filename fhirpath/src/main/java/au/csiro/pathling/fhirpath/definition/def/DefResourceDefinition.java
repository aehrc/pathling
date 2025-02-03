package au.csiro.pathling.fhirpath.definition.def;

import au.csiro.pathling.fhirpath.definition.ChildDefinition;
import au.csiro.pathling.fhirpath.definition.ResourceDefinition;
import jakarta.annotation.Nonnull;
import lombok.Value;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import java.util.List;
import java.util.Optional;

@Value(staticConstructor = "of")
public class DefResourceDefinition implements ResourceDefinition {

  List<ChildDefinition> children;

  @Override
  @Nonnull
  public ResourceType getResourceType() {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  @Nonnull
  public Optional<? extends ChildDefinition> getChildElement(@Nonnull String name) {
    return children.stream()
        .filter(child -> child.getName().equals(name))
        .findFirst();
  }

  @Nonnull
  public DefResourceDefinition of(ChildDefinition... children) {
    return DefResourceDefinition.of(List.of(children));
  }
}
