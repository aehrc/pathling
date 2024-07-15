package au.csiro.pathling.fhirpath;

import au.csiro.pathling.fhirpath.collection.Collection;
import jakarta.annotation.Nonnull;

/**
 * A description of how to take one {@link Collection} and transform it into another.
 *
 * @author John Grimes
 */
@FunctionalInterface
public interface FhirPath {

  Collection apply(@Nonnull final Collection input, @Nonnull final EvaluationContext context);

}
