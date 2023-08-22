package au.csiro.pathling.fhirpath;

import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import javax.annotation.Nonnull;
import lombok.Value;

/**
 * Holds the value of a {@link Collection} and an associated {@link ParserContext}.
 *
 * @author John Grimes
 */
@Value
public class FhirPathAndContext {

  @Nonnull
  Collection result;

  @Nonnull
  ParserContext context;

}
