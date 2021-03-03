package au.csiro.pathling.terminology;

import static au.csiro.pathling.utilities.Preconditions.checkNotNull;

import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.fhirpath.encoding.SimpleCoding;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Enumerations.ConceptMapEquivalence;

/**
 * Default implementation of TerminologyService using a backed terminology server.
 */
public class DefaultTerminologyService implements TerminologyService {


  @Nonnull
  private final TerminologyClient terminologyClient;

  public DefaultTerminologyService(@Nonnull TerminologyClient terminologyClient) {
    this.terminologyClient = terminologyClient;
  }

  @Nonnull
  @Override
  public ConceptTranslator translate(@Nonnull final Collection<SimpleCoding> codings,
      @Nonnull final String conceptMapUrl, final boolean reverse,
      @Nonnull final Collection<ConceptMapEquivalence> equivalences) {

    final List<SimpleCoding> uniqueCodings = codings.stream().distinct()
        .collect(Collectors.toUnmodifiableList());
    // create bundle
    final Bundle translateBatch = TranslateMapping
        .toRequestBundle(uniqueCodings, conceptMapUrl, reverse);
    final Bundle result = terminologyClient.batch(translateBatch);
    return TranslateMapping.fromResponseBundle(checkNotNull(result), uniqueCodings, equivalences);
  }
}
