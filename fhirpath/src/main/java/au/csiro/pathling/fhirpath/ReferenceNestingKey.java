package au.csiro.pathling.fhirpath;

import au.csiro.pathling.fhirpath.definition.ElementDefinition;
import au.csiro.pathling.fhirpath.definition.ResourceDefinition;
import jakarta.annotation.Nonnull;
import lombok.Value;

/**
 * A specialisation of a {@link NestingKey} that is used to store the details of a resource
 * reference.
 *
 * @author John Grimes
 */
@Value
public class ReferenceNestingKey implements NestingKey {

  @Nonnull
  ElementDefinition referenceDefinition;

  @Nonnull
  ResourceDefinition resourceDefinition;

}
