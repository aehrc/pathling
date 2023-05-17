package au.csiro.pathling.fhirpath;

import au.csiro.pathling.fhirpath.parser.ParserContext;
import javax.annotation.Nonnull;
import lombok.Value;

@Value
public class FhirPathAndContext {

  @Nonnull
  FhirPath fhirPath;

  @Nonnull
  ParserContext context;

}
