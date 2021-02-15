/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function.translate;

import static au.csiro.pathling.fhirpath.TerminologyUtils.streamOf;

import au.csiro.pathling.fhir.TerminologyClientFactory;
import au.csiro.pathling.fhirpath.encoding.CodingEncoding;
import au.csiro.pathling.fhirpath.encoding.SimpleCoding;
import au.csiro.pathling.sql.MapperWithPreview;
import au.csiro.pathling.terminology.ConceptMapper;
import au.csiro.pathling.terminology.TerminologyService;
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

/**
 * Takes a list of {@link SimpleCoding} and returns a Boolean result indicating if any of the
 * codings belongs to the specified ValueSet.
 */
@Slf4j
public class TranslatefMapperWithPreview implements
    MapperWithPreview<List<SimpleCoding>, Row[], ConceptMapper> {

  private static final long serialVersionUID = 2879761794073649202L;

  @Nonnull
  private final String requestId;

  @Nonnull
  private final TerminologyClientFactory terminologyClientFactory;

  @Nonnull
  private final String conceptMapUrl;

  private final boolean reverse;

  @Nonnull
  private final List<ConceptMapEquivalence> equivalences;


  /**
   * @param requestId An identifier used alongside any logging that the mapper outputs
   * @param terminologyClientFactory Used to create instances of the terminology client on workers
   */
  public TranslatefMapperWithPreview(@Nonnull final String requestId,
      @Nonnull final TerminologyClientFactory terminologyClientFactory,
      @Nonnull final String conceptMapUrl, final boolean reverse,
      @Nonnull final List<ConceptMapEquivalence> equivalences) {
    this.requestId = requestId;
    this.terminologyClientFactory = terminologyClientFactory;
    this.conceptMapUrl = conceptMapUrl;
    this.reverse = reverse;
    this.equivalences = equivalences;
  }

  @Override
  @Nonnull
  public ConceptMapper preview(@Nonnull final Iterator<List<SimpleCoding>> input) {
    if (!input.hasNext() || equivalences.isEmpty()) {
      return new ConceptMapper();
    }
    final Set<SimpleCoding> uniqueCodings = streamOf(input)
        .filter(Objects::nonNull)
        .flatMap(List::stream)
        .collect(Collectors.toSet());
    final TerminologyService terminologyService = terminologyClientFactory.buildService(log);
    return terminologyService.translate(uniqueCodings, conceptMapUrl,
        reverse, equivalences);
  }

  @Override
  @Nullable
  public Row[] call(@Nullable final List<SimpleCoding> input,
      @Nonnull final ConceptMapper state) {

    final List<Coding> outputCodings = state.translate(input);
    return outputCodings.isEmpty()
           ? null
           : CodingEncoding.encodeList(outputCodings);

  }
}
