package au.csiro.pathling.terminology;

import au.csiro.pathling.fhirpath.encoding.SimpleCoding;
import java.util.Collection;
import javax.annotation.Nonnull;
import org.hl7.fhir.r4.model.Enumerations.ConceptMapEquivalence;

public interface TerminologyService {

  @Nonnull
  ConceptMapper translate(@Nonnull Collection<SimpleCoding> codings, @Nonnull String conceptMapUrl,
      boolean reverse, @Nonnull Collection<ConceptMapEquivalence> equivalences);
}
