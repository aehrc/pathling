package au.csiro.pathling.fhirpath;

import au.csiro.pathling.fhirpath.parser.ParserContext;
import javax.annotation.Nonnull;
import lombok.Value;

/**
 * Holds the value of a {@link FhirPath} and an associated {@link ParserContext}.
 *
 * @author John Grimes
 */
@Value
public class FhirPathAndContext {

  @Nonnull
  FhirPath fhirPath;

  @Nonnull
  ParserContext context;

}
