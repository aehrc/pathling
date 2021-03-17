/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.terminology;

import static au.csiro.pathling.utilities.Preconditions.checkNotNull;

import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.fhirpath.encoding.SimpleCoding;
import ca.uhn.fhir.context.FhirContext;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Enumerations.ConceptMapEquivalence;

/**
 * Default implementation of TerminologyService using a backend terminology server.
 */
public class DefaultTerminologyService implements TerminologyService {


  @Nonnull
  private final FhirContext fhirContext;

  @Nonnull
  private final TerminologyClient terminologyClient;

  /**
   * @param fhirContext The {@link FhirContext} used to interpret responses
   * @param terminologyClient The {@link TerminologyClient} used to issue requests
   */
  public DefaultTerminologyService(@Nonnull final FhirContext fhirContext,
      @Nonnull final TerminologyClient terminologyClient) {
    this.fhirContext = fhirContext;
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
    return TranslateMapping
        .fromResponseBundle(checkNotNull(result), uniqueCodings, equivalences, fhirContext);
  }
}
