package au.csiro.pathling.terminology;

import au.csiro.pathling.terminology.TranslateMapping.TranslationEntry;
import lombok.Value;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.codesystems.ConceptMapEquivalence;
import org.hl7.fhir.r4.model.codesystems.ConceptSubsumptionOutcome;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

public interface TerminologyService2 {

  @Value(staticConstructor = "of")
  class Translation {

    @Nonnull
    ConceptMapEquivalence equivalence;

    @Nonnull
    Coding concept;
  }


  boolean validate(@Nonnull String url, @Nonnull Coding coding);

  @Nonnull
  List<Translation> translate(@Nonnull Coding coding,
      @Nonnull String conceptMapUrl,
      boolean reverse,
      @Nullable String target);

  @Nonnull
  ConceptSubsumptionOutcome subsumes(@Nonnull Coding codingA, @Nonnull Coding codingB);
}
