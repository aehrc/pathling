package au.csiro.pathling.fhirpath;

import javax.annotation.Nonnull;

public interface NodeDefinition {
  
  @Nonnull
  NestingKey getNestingKey();

}
