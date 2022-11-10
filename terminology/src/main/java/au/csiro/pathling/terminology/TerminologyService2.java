package au.csiro.pathling.terminology;

import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Parameters;
import javax.annotation.Nonnull;

public interface TerminologyService2 {

  @Nonnull
  Parameters validate(@Nonnull String url, @Nonnull Coding coding);

  @Nonnull
  Parameters translateCoding(@Nonnull Coding coding, @Nonnull String conceptMapUrl,
      boolean reverse);

}
