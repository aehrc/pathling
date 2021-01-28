package au.csiro.pathling.fhirpath.parser;

import au.csiro.pathling.fhirpath.FhirPath;
import javax.annotation.Nonnull;
import lombok.Data;

@Data
public class ParserResult {

  @Nonnull
  private final FhirPath fhirPath;
  @Nonnull
  private final ParserContext evaluationContext;

  @Nonnull
  public  ParserResult withPath(@Nonnull final FhirPath newPath) {
    return new ParserResult(newPath, evaluationContext);
  }

}
