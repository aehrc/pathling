/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function.subsumes;

import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.fhir.TerminologyClientFactory;
import au.csiro.pathling.fhirpath.encoding.BooleanResult;
import au.csiro.pathling.fhirpath.encoding.IdAndCodingSets;
import au.csiro.pathling.fhirpath.encoding.SimpleCoding;
import ca.uhn.fhir.rest.param.UriParam;
import com.google.common.collect.Streams;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.hl7.fhir.r4.model.CodeSystem;
import org.slf4j.MDC;


/**
 * Takes a set of Rows with schema: STRING id, ARRAY(CODING) inputCoding, ARRAY(CODING) argCodings
 * to check for a subsumption relation with a terminology server.
 * <p>
 * Returns a set of {@link BooleanResult} objects, which contain the identified and the status of
 * subsumption relation for each of input elements.
 */
@Slf4j
public class SubsumptionMapper
    implements MapPartitionsFunction<IdAndCodingSets, BooleanResult> {

  private static final long serialVersionUID = 1L;

  @Nonnull
  private final String requestId;

  @Nonnull
  private final TerminologyClientFactory terminologyClientFactory;
  private final boolean inverted;

  /**
   * Constructor
   *
   * @param requestId An identifier used alongside any logging that the mapper outputs
   * @param terminologyClientFactory the factory to use to create the {@link
   * au.csiro.pathling.fhir.TerminologyClient}
   * @param inverted if true checks for `subsumedBy` relation otherwise for `subsumes`
   */
  public SubsumptionMapper(@Nonnull final String requestId,
      @Nonnull final TerminologyClientFactory terminologyClientFactory,
      final boolean inverted) {
    this.requestId = requestId;
    this.terminologyClientFactory = terminologyClientFactory;
    this.inverted = inverted;
  }

  @Override
  public Iterator<BooleanResult> call(@Nonnull final Iterator<IdAndCodingSets> input) {
    final Iterable<IdAndCodingSets> inputIterable = () -> input;
    final List<IdAndCodingSets> entries = StreamSupport
        .stream(inputIterable.spliterator(), false)
        .collect(Collectors.toList());

    // Add the request ID to the logging context, so that we can track the logging for this
    // request across all workers.
    MDC.put("requestId", requestId);

    // Collect all distinct tokens used on both in inputs and arguments in this partition
    // Rows in which either input or argument are NULL are excluded as they do not need
    // to be included in closure request.
    // Also we only include codings with both system and code defined as only they can
    // be expected to be meaningfully represented to the terminology server

    final Set<SimpleCoding> allCodings = entries.stream()
        .filter(r -> r.getInputCodings() != null && r.getArgCodings() != null)
        .flatMap(r -> Streams.concat(r.getInputCodings().stream(), r.getArgCodings().stream()))
        .filter(SimpleCoding::isDefined)
        .collect(Collectors.toSet());

    final TerminologyClient terminologyClient = terminologyClientFactory.build(log);

    // filter out codings with code systems unknown to the terminology server
    final Set<String> allCodeSystems = allCodings.stream()
        .map(SimpleCoding::getSystem)
        .collect(Collectors.toSet());

    final Set<String> knownCodeSystems = allCodeSystems.stream().filter(codeSystem -> {
      final UriParam uri = new UriParam(codeSystem);
      final List<CodeSystem> knownSystems = terminologyClient.searchCodeSystems(
          uri, new HashSet<>(Collections.singletonList("id")));
      return !knownSystems.isEmpty();
    }).collect(Collectors.toSet());

    if (!knownCodeSystems.equals(allCodeSystems)) {
      final Collection<String> unrecognizedCodeSystems = new HashSet<>(allCodeSystems);
      unrecognizedCodeSystems.removeAll(knownCodeSystems);
      log.warn("Terminology server does not recognize these coding systems: {}",
          unrecognizedCodeSystems);
    }

    final Set<SimpleCoding> knownCodings = allCodings.stream()
        .filter(coding -> knownCodeSystems.contains(coding.getSystem()))
        .collect(Collectors.toSet());

    final ClosureService closureService = new ClosureService(terminologyClient);
    final Closure subsumeClosure = closureService.getSubsumesRelation(knownCodings);

    return entries.stream().map(r -> {
      if (r.getInputCodings() == null) {
        return BooleanResult.nullOf(r.getId());
      } else {
        final boolean result = (!inverted
                                ? subsumeClosure
                                    .anyRelates(r.safeGetInputCodings(), r.safeGetArgCodings())
                                :
                                subsumeClosure
                                    .anyRelates(r.safeGetArgCodings(), r.safeGetInputCodings()));
        return BooleanResult.of(r.getId(), result);
      }
    }).iterator();
  }
}