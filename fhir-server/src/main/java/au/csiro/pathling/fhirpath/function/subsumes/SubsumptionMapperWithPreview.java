/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function.subsumes;

import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.fhir.TerminologyClientFactory;
import au.csiro.pathling.fhirpath.encoding.SimpleCoding;
import au.csiro.pathling.sql.MapperWithPreview;
import ca.uhn.fhir.rest.param.UriParam;
import com.google.common.collect.Streams;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.hl7.fhir.r4.model.CodeSystem;
import org.slf4j.MDC;

/**
 * Takes a pair of {@link List} of {@link SimpleCoding}. Returns a Boolean value indicating if any
 * coding in the left list is in the subsumption relation with any coding in the right list.
 */
@Slf4j
public class SubsumptionMapperWithPreview implements
    MapperWithPreview<ImmutablePair<
        List<SimpleCoding>, List<SimpleCoding>>, Boolean, Closure> {

  private static final long serialVersionUID = 2879761794073649202L;

  @Nonnull
  private final String requestId;

  @Nonnull
  private final TerminologyClientFactory terminologyClientFactory;
  private final boolean inverted;


  /**
   * @param requestId an identifier used alongside any logging that the mapper outputs
   * @param terminologyClientFactory the factory to use to create the {@link
   * au.csiro.pathling.fhir.TerminologyClient}
   * @param inverted if true checks for `subsumedBy` relation, otherwise for `subsumes`
   */
  public SubsumptionMapperWithPreview(@Nonnull final String requestId,
      @Nonnull final TerminologyClientFactory terminologyClientFactory,
      final boolean inverted) {
    this.requestId = requestId;
    this.terminologyClientFactory = terminologyClientFactory;
    this.inverted = inverted;
  }

  @Override
  @Nonnull
  public Closure preview(
      @Nonnull final Iterator<ImmutablePair<List<SimpleCoding>, List<SimpleCoding>>> input) {

    // Add the request ID to the logging context, so that we can track the logging for this
    // request across all workers.
    MDC.put("requestId", requestId);

    final Iterable<ImmutablePair<List<SimpleCoding>, List<SimpleCoding>>> inputRowsIterable = () -> input;
    final Stream<ImmutablePair<List<SimpleCoding>, List<SimpleCoding>>> inputStream = StreamSupport
        .stream(inputRowsIterable.spliterator(), false);

    // Collect all distinct tokens used on both in inputs and arguments in this partition
    // Rows in which either input or argument are NULL are excluded as they do not need
    // to be included in closure request.
    // Also we only include codings with both system and code defined as only they can
    // be expected to be meaningfully represented to the terminology server

    final Set<SimpleCoding> allCodings = inputStream
        .filter(r -> r.getLeft() != null && r.getRight() != null)
        .flatMap(r -> Streams.concat(r.getLeft().stream(), r.getRight().stream()))
        .filter(Objects::nonNull)
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
    return closureService.getSubsumesRelation(knownCodings);
  }

  @Nonnull
  private static <T> List<T> safeGetList(@Nullable final List<T> list) {
    return (list != null)
           ? list
           : Collections.emptyList();
  }

  @Override
  @Nullable
  public Boolean call(@Nullable final ImmutablePair<List<SimpleCoding>, List<SimpleCoding>> input,
      @Nonnull final Closure subsumeClosure) {

    if (Objects.isNull(input) || Objects.isNull(input.getLeft())) {
      return null;
    }

    @Nonnull final List<SimpleCoding> inputCodings = safeGetList(input.getLeft());
    @Nonnull final List<SimpleCoding> argCodings = safeGetList(input.getRight());

    return (!inverted
            ? subsumeClosure
                .anyRelates(inputCodings, argCodings)
            :
            subsumeClosure
                .anyRelates(argCodings,
                    inputCodings));

  }
}
