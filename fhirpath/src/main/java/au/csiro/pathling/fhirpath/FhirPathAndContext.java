package au.csiro.pathling.fhirpath;

import au.csiro.pathling.fhirpath.collection.Collection;
import javax.annotation.Nonnull;
import lombok.Value;

/**
 * Holds the value of a {@link Collection} and an associated {@link EvaluationContext}.
 *
 * @author John Grimes
 */
@Value
public class FhirPathAndContext {

  @Nonnull
  Collection result;

  @Nonnull
  EvaluationContext context;

}
