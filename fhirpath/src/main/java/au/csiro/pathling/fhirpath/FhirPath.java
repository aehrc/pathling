package au.csiro.pathling.fhirpath;

import au.csiro.pathling.fhirpath.collection.Collection;
import java.util.function.Function;

/**
 * A description of how to take one {@link Collection} and transform it into another.
 *
 * @param <I> The input type of {@link Collection}
 * @param <O> The output type of {@link Collection}
 * @author John Grimes
 */
public interface FhirPath<I extends Collection, O extends Collection> extends Function<I, O> {

}
