package au.csiro.pathling.terminology;

import au.csiro.pathling.terminology.TerminologyService.PropertyOrDesignation;
import java.io.Serial;
import java.util.ArrayList;

/**
 * A list of properties or designations, used to represent the results of a terminology property or
 * designation operation. This class extends {@link ArrayList} to provide a convenient way to
 * handle collections of properties or designations.
 *
 * @author John Grimes
 */
public class PropertyOrDesignationList extends ArrayList<PropertyOrDesignation> {

  @Serial
  private static final long serialVersionUID = 5991920759756980001L;
}
