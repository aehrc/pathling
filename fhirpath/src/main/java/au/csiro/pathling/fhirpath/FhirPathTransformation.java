package au.csiro.pathling.fhirpath;

import java.util.function.UnaryOperator;

/**
 * A description of how to take one {@link FhirPath} and transform it into another.
 *
 * @author John Grimes
 */
public interface FhirPathTransformation extends UnaryOperator<FhirPath> {

}
