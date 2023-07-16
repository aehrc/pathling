package au.csiro.pathling.fhirpath;

import au.csiro.pathling.fhirpath.element.ElementDefinition;
import javax.annotation.Nonnull;
import lombok.Value;

@Value
public class ReferenceNestingKey implements NestingKey {

  @Nonnull
  ElementDefinition referenceDefinition;

  @Nonnull
  ResourceDefinition resourceDefinition;

}
