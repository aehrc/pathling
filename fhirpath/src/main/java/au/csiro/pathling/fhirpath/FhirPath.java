package au.csiro.pathling.fhirpath;

import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.evaluation.EvaluationContext;
import au.csiro.pathling.fhirpath.expression.Traversal;
import java.util.Optional;

/**
 * A description of how to take one {@link Collection} and transform it into another.
 *
 * @author John Grimes
 */
@FunctionalInterface
public interface FhirPath {

  Collection evaluate(final Collection input, final EvaluationContext context);

  default FhirPath traverse(final String target) {
    return new Traversal(Optional.of(this), target);
  }

}
