package au.csiro.pathling.terminology;

import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Parameters;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface TerminologyService2 {

  boolean validate(@Nonnull String url, @Nonnull Coding coding);

  @Nonnull
  Parameters translate(@Nonnull Coding coding, @Nonnull String conceptMapUrl,
      boolean reverse);

  @Nullable
  Parameters subsumes(@Nonnull Coding codingA, @Nonnull Coding codingB);
}
