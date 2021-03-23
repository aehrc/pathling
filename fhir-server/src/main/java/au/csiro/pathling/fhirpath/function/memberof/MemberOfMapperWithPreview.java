/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function.memberof;

import au.csiro.pathling.fhir.TerminologyClientFactory;
import au.csiro.pathling.fhirpath.encoding.SimpleCoding;
import au.csiro.pathling.sql.MapperWithPreview;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.utilities.Streams;
import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;

/**
 * Takes a list of {@link SimpleCoding} and returns a Boolean result indicating if any of the
 * codings belongs to the specified ValueSet.
 */
@Slf4j
public class MemberOfMapperWithPreview implements
    MapperWithPreview<List<SimpleCoding>, Boolean, Set<SimpleCoding>> {

  private static final long serialVersionUID = 2879761794073649202L;

  @Nonnull
  private final String requestId;

  @Nonnull
  private final TerminologyClientFactory terminologyClientFactory;

  @Nonnull
  private final String valueSetUri;

  /**
   * @param requestId An identifier used alongside any logging that the mapper outputs
   * @param terminologyClientFactory Used to create instances of the terminology client on workers
   * @param valueSetUri The identifier of the ValueSet that codes will be validated against
   */
  public MemberOfMapperWithPreview(@Nonnull final String requestId,
      @Nonnull final TerminologyClientFactory terminologyClientFactory,
      @Nonnull final String valueSetUri) {
    this.requestId = requestId;
    this.terminologyClientFactory = terminologyClientFactory;
    this.valueSetUri = valueSetUri;
  }

  @Override
  @Nonnull
  public Set<SimpleCoding> preview(@Nonnull final Iterator<List<SimpleCoding>> input) {
    if (!input.hasNext()) {
      return Collections.emptySet();
    }
    MDC.put("requestId", requestId);

    final Set<SimpleCoding> codings = Streams.streamOf(input)
        .filter(Objects::nonNull)
        .flatMap(List::stream)
        .collect(Collectors.toSet());

    final TerminologyService terminologyService = terminologyClientFactory.buildService(log);
    return terminologyService.intersect(valueSetUri, codings);
  }

  @Override
  @Nullable
  public Boolean call(@Nullable final List<SimpleCoding> input,
      @Nonnull final Set<SimpleCoding> state) {
    return input != null
           ? !Collections.disjoint(state, input)
           : null;
  }

  @Value
  private static class CodeSystemReference {

    @Nonnull
    Optional<String> system;

    @Nonnull
    Optional<String> version;

    private boolean matchesCoding(@Nonnull final SimpleCoding coding) {
      if (system.isEmpty() || coding.getSystem() == null) {
        return false;
      }
      final boolean eitherSideIsMissingVersion =
          version.isEmpty() || coding.getVersion() == null;
      final boolean versionAgnosticTest = system.get().equals(coding.getSystem());
      if (eitherSideIsMissingVersion) {
        return versionAgnosticTest;
      } else {
        return versionAgnosticTest && version.get().equals(coding.getVersion());
      }
    }
  }

}
