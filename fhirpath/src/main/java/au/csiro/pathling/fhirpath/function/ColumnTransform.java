package au.csiro.pathling.fhirpath.function;

import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import java.util.function.Function;

/**
 * Represents a transformation from one {@link ColumnRepresentation} to another.
 *
 * @author John Grimes
 */
public interface ColumnTransform extends Function<ColumnRepresentation, ColumnRepresentation> {

}
