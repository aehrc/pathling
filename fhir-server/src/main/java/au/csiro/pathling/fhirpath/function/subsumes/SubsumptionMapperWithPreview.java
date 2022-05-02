/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function.subsumes;

import static java.util.stream.Stream.concat;

import au.csiro.pathling.fhir.TerminologyServiceFactory;
import au.csiro.pathling.fhirpath.encoding.SimpleCoding;
import au.csiro.pathling.sql.MapperWithPreview;
import au.csiro.pathling.terminology.Relation;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.utilities.Streams;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.MDC;

/**
 * Takes a pair of {@link List} of {@link SimpleCoding}. Returns a Boolean value indicating if any
 * coding in the left list is in the subsumption relation with any coding in the right list.
 */
@Slf4j
public class SubsumptionMapperWithPreview implements
    MapperWithPreview<ImmutablePair<
        List<SimpleCoding>, List<SimpleCoding>>, Boolean, Relation> {

  private static final long serialVersionUID = 2879761794073649202L;

  @Nonnull
  private final String requestId;

  @Nonnull
  private final TerminologyServiceFactory terminologyServiceFactory;
  private final boolean inverted;


  /**
   * @param requestId an identifier used alongside any logging that the mapper outputs
   * @param terminologyServiceFactory the factory to use to create the {@link
   * au.csiro.pathling.fhir.TerminologyClient}
   * @param inverted if true checks for `subsumedBy` relation, otherwise for `subsumes`
   */
  public SubsumptionMapperWithPreview(@Nonnull final String requestId,
      @Nonnull final TerminologyServiceFactory terminologyServiceFactory,
      final boolean inverted) {
    this.requestId = requestId;
    this.terminologyServiceFactory = terminologyServiceFactory;
    this.inverted = inverted;
  }

  @Override
  @Nonnull
  public Relation preview(
      @Nonnull final Iterator<ImmutablePair<List<SimpleCoding>, List<SimpleCoding>>> input) {

    // Add the request ID to the logging context, so that we can track the logging for this
    // request across all workers.
    MDC.put("requestId", requestId);

    // Collect all distinct tokens used on both in inputs and arguments in this partition
    // Rows in which either input or argument are NULL are excluded as they do not need
    // to be included in closure request.
    // Also, we only include codings with both system and code defined as only they can
    // be expected to be meaningfully represented to the terminology server

    final Set<SimpleCoding> codings = Streams.streamOf(input)
        .filter(r -> r.getLeft() != null && r.getRight() != null)
        .flatMap(r -> concat(r.getLeft().stream(), r.getRight().stream()))
        .filter(Objects::nonNull)
        .collect(Collectors.toSet());

    final TerminologyService terminologyService = terminologyServiceFactory.buildService(log);
    return terminologyService.getSubsumesRelation(codings);
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
      @Nonnull final Relation subsumeRelation) {

    if (Objects.isNull(input) || Objects.isNull(input.getLeft())) {
      return null;
    }

    @Nonnull final List<SimpleCoding> inputCodings = safeGetList(input.getLeft());
    @Nonnull final List<SimpleCoding> argCodings = safeGetList(input.getRight());

    return (!inverted
            ? subsumeRelation
                .anyRelates(inputCodings, argCodings)
            :
            subsumeRelation
                .anyRelates(argCodings,
                    inputCodings));

  }
}
