/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function.translate;

import au.csiro.pathling.fhir.TerminologyServiceFactory;
import au.csiro.pathling.fhirpath.encoding.CodingEncoding;
import au.csiro.pathling.fhirpath.encoding.SimpleCoding;
import au.csiro.pathling.sql.MapperWithPreview;
import au.csiro.pathling.terminology.ConceptTranslator;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.utilities.Streams;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.ConceptMapEquivalence;
import org.slf4j.MDC;

/**
 * Takes a list of {@link SimpleCoding} and returns a Boolean result indicating if any of the
 * codings belongs to the specified ValueSet.
 */
@Slf4j
public class TranslateMapperWithPreview implements
    MapperWithPreview<List<SimpleCoding>, Row[], ConceptTranslator> {

  private static final long serialVersionUID = 2879761794073649202L;

  @Nonnull
  private final String requestId;

  @Nonnull
  private final TerminologyServiceFactory terminologyServiceFactory;

  @Nonnull
  private final String conceptMapUrl;

  private final boolean reverse;

  @Nonnull
  private final List<ConceptMapEquivalence> equivalences;


  /**
   * @param requestId An identifier used alongside any logging that the mapper outputs
   * @param terminologyServiceFactory Used to create instances of the terminology client on workers
   * @param conceptMapUrl The URI of the ConceptMap to use for translations
   * @param reverse If set, reverse source and target within the map
   * @param equivalences The list of equivalence values that will be matched
   */
  public TranslateMapperWithPreview(@Nonnull final String requestId,
      @Nonnull final TerminologyServiceFactory terminologyServiceFactory,
      @Nonnull final String conceptMapUrl, final boolean reverse,
      @Nonnull final List<ConceptMapEquivalence> equivalences) {
    this.requestId = requestId;
    this.terminologyServiceFactory = terminologyServiceFactory;
    this.conceptMapUrl = conceptMapUrl;
    this.reverse = reverse;
    this.equivalences = equivalences;
  }

  @Override
  @Nonnull
  public ConceptTranslator preview(@Nonnull final Iterator<List<SimpleCoding>> input) {
    if (!input.hasNext() || equivalences.isEmpty()) {
      return new ConceptTranslator();
    }

    // Add the request ID to the logging context, so that we can track the logging for this
    // request across all workers.
    MDC.put("requestId", requestId);

    final Set<SimpleCoding> uniqueCodings = Streams.streamOf(input)
        .filter(Objects::nonNull)
        .flatMap(List::stream)
        .collect(Collectors.toSet());
    final TerminologyService terminologyService = terminologyServiceFactory.buildService(log);
    return terminologyService.translate(uniqueCodings, conceptMapUrl,
        reverse, equivalences);
  }

  @Override
  @Nullable
  public Row[] call(@Nullable final List<SimpleCoding> input,
      @Nonnull final ConceptTranslator state) {

    final List<Coding> outputCodings = state.translate(input);
    return outputCodings.isEmpty()
           ? null
           : CodingEncoding.encodeList(outputCodings);

  }
}
