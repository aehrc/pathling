/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.csiro.pathling.terminology.expand;

import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.terminology.TerminologyServerReasons;
import ca.uhn.fhir.rest.client.exceptions.FhirClientConnectionException;
import ca.uhn.fhir.rest.server.exceptions.BaseServerResponseException;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Optional;
import java.util.function.IntFunction;
import org.hl7.fhir.r4.model.IntegerType;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.UriType;
import org.hl7.fhir.r4.model.ValueSet;
import org.hl7.fhir.r4.model.ValueSet.ValueSetExpansionComponent;
import org.hl7.fhir.r4.model.ValueSet.ValueSetExpansionContainsComponent;

/**
 * Expands a value set through the FHIR {@code ValueSet/$expand} operation of a terminology server,
 * paging through the expansion until it is complete.
 *
 * <p>Pages are requested with {@link #EXPAND_PAGE_SIZE} entries and an offset that advances by the
 * number of entries each page actually returned, so that a server which caps the page size still
 * yields a complete membership. Paging stops when the offset reaches the expansion's {@code total},
 * or where no total is reported, when a page returns fewer entries than requested or none. The
 * members of every page pass through the same flattening as a supplied expansion, deduplicated
 * across pages. A page whose {@code total} exceeds the caller's limit is rejected at once, without
 * fetching the rest, so an oversized expansion costs one request; {@code total} is trusted for this
 * even though abstract or duplicate entries would contribute no member. Otherwise the limit is
 * checked after each page so that no page beyond the one that reveals the excess is fetched.
 *
 * <p>A failure of the first page of a canonical expansion, other than a 404, is reported as a
 * {@link ValueSetLookupException}, because until a page has been returned it is unknown whether the
 * URL names a value set at all. A failure of a later page, or of the expansion of a supplied
 * resource, is a fault in a value set that is known to exist and keeps the plain {@link
 * ValueSetExpansionException}.
 *
 * @author John Grimes
 */
public class ExpandExecutor {

  /** The number of entries requested per page. */
  public static final int EXPAND_PAGE_SIZE = 10000;

  @Nonnull private final TerminologyClient terminologyClient;

  /**
   * Creates an executor over a terminology client.
   *
   * @param terminologyClient the client for communicating with the terminology server
   */
  public ExpandExecutor(@Nonnull final TerminologyClient terminologyClient) {
    this.terminologyClient = terminologyClient;
  }

  /**
   * Expands a value set identified by canonical URL.
   *
   * @param url the canonical URL of the value set, without a version suffix
   * @param version the value set version to expand, or null for the server's default
   * @param maxMembers the largest membership the caller will accept
   * @return the expansion, or empty if the server does not know the canonical URL
   * @throws ValueSetLookupException if the server answers the first page with a failure status
   *     other than 404, or cannot be reached for it, so that it is unknown whether the URL is a
   *     value set
   * @throws ValueSetExpansionException if the server cannot expand the value set once a page has
   *     been returned, or the expansion is not a valid membership
   * @throws ExpansionLimitExceededException if the membership exceeds {@code maxMembers}
   */
  @Nonnull
  public Optional<ValueSetExpansion> expand(
      @Nonnull final String url, @Nullable final String version, final int maxMembers) {
    final UriType urlParameter = new UriType(url);
    final StringType versionParameter = version == null ? null : new StringType(version);
    try {
      return Optional.of(
          page(
              offset ->
                  terminologyClient.expand(
                      urlParameter,
                      versionParameter,
                      new IntegerType(EXPAND_PAGE_SIZE),
                      new IntegerType(offset)),
              url,
              version,
              maxMembers,
              true));
    } catch (final ResourceNotFoundException e) {
      // The server does not know the canonical URL, so the value set cannot be resolved.
      return Optional.empty();
    }
  }

  /**
   * Expands a value set supplied as a resource. Where the resource carries an expansion, that
   * expansion is the membership; otherwise the resource is sent to the server for expansion.
   *
   * @param valueSet the value set resource
   * @param maxMembers the largest membership the caller will accept
   * @return the expansion
   * @throws ValueSetExpansionException if the membership cannot be determined
   * @throws ExpansionLimitExceededException if the membership exceeds {@code maxMembers}
   */
  @Nonnull
  public ValueSetExpansion expand(@Nonnull final ValueSet valueSet, final int maxMembers) {
    if (valueSet.hasExpansion()) {
      return ValueSetExpansion.fromResource(valueSet, maxMembers);
    }
    final String url = ValueSetExpansion.urlOf(valueSet);
    final String version = valueSet.hasVersion() ? valueSet.getVersion() : null;
    try {
      return page(
          offset ->
              terminologyClient.expand(
                  valueSet, new IntegerType(EXPAND_PAGE_SIZE), new IntegerType(offset)),
          url,
          version,
          maxMembers,
          false);
    } catch (final ResourceNotFoundException e) {
      // The resource was supplied, so a 404 means the server could not expand it rather than that
      // the value set is unknown.
      throw serverFailure(e);
    }
  }

  /**
   * Pages through an expansion, accumulating members until it is complete.
   *
   * @param request issues the request for the page starting at the given offset
   * @param requestedUrl the canonical URL the caller asked for, used where the server reports none
   * @param requestedVersion the version the caller asked for, used where the server reports none
   * @param maxMembers the largest membership the caller will accept
   * @param canonical whether the expansion is of a canonical URL rather than a supplied resource
   * @return the complete expansion
   * @throws ResourceNotFoundException if the server answers a page with 404
   */
  @Nonnull
  private ValueSetExpansion page(
      @Nonnull final IntFunction<ValueSet> request,
      @Nonnull final String requestedUrl,
      @Nullable final String requestedVersion,
      final int maxMembers,
      final boolean canonical) {
    final ExpansionAccumulator accumulator = new ExpansionAccumulator(maxMembers);
    ValueSet first = null;
    int offset = 0;
    while (true) {
      final ValueSet page = fetch(request, offset, canonical);
      if (!page.hasExpansion()) {
        throw new ValueSetExpansionException(
            "the terminology server returned a value set with no expansion");
      }
      if (first == null) {
        first = page;
      }
      final ValueSetExpansionComponent expansion = page.getExpansion();
      if (expansion.hasTotal() && expansion.getTotal() > maxMembers) {
        throw new ExpansionLimitExceededException(maxMembers);
      }
      final List<ValueSetExpansionContainsComponent> entries = expansion.getContains();
      accumulator.addContains(entries);
      accumulator.checkLimit();
      if (entries.isEmpty()) {
        break;
      }
      offset += entries.size();
      if (expansion.hasTotal()) {
        if (offset >= expansion.getTotal()) {
          break;
        }
      } else if (entries.size() < EXPAND_PAGE_SIZE) {
        break;
      }
    }
    final ValueSetExpansionComponent expansion = first.getExpansion();
    return new ValueSetExpansion(
        first.hasUrl() ? first.getUrl() : requestedUrl,
        first.hasVersion() ? first.getVersion() : requestedVersion,
        ValueSetExpansion.identifierOf(expansion),
        ValueSetExpansion.timestampOf(expansion),
        ValueSetExpansion.codeSystemVersionsOf(expansion),
        accumulator.members());
  }

  /**
   * Requests one page, translating a failure to reach or be answered by the server. Where the page
   * is the first of a canonical expansion, a failure leaves it unknown whether the URL names a
   * value set at all, and is reported as a {@link ValueSetLookupException}; once a page has been
   * returned the value set exists, so a later failure is a fault in it.
   *
   * @param request issues the request for the page starting at the given offset
   * @param offset the index of the first entry requested
   * @param canonical whether the expansion is of a canonical URL rather than a supplied resource
   * @return the page
   * @throws ResourceNotFoundException if the server answers 404, for the caller to interpret
   * @throws ValueSetLookupException if the first page of a canonical expansion cannot be fetched
   *     because the server cannot be reached or answers with any other failure
   * @throws ValueSetExpansionException if any other page cannot be fetched for the same reasons
   */
  @Nonnull
  private ValueSet fetch(
      @Nonnull final IntFunction<ValueSet> request, final int offset, final boolean canonical) {
    final boolean lookup = canonical && offset == 0;
    try {
      return request.apply(offset);
    } catch (final FhirClientConnectionException e) {
      final String reason = TerminologyServerReasons.unreachable(terminologyClient.getServerUrl());
      throw lookup
          ? new ValueSetLookupException(reason, e, true)
          : new ValueSetExpansionException(reason, e);
    } catch (final ResourceNotFoundException e) {
      throw e;
    } catch (final BaseServerResponseException e) {
      throw lookup
          ? new ValueSetLookupException(TerminologyServerReasons.httpFailure(e), e, false)
          : serverFailure(e);
    }
  }

  /**
   * Builds the exception for a server response that is not a success, carrying the reason given by
   * the response's OperationOutcome where it has one and the HTTP status otherwise.
   *
   * @param e the response exception
   * @return the exception to throw
   */
  @Nonnull
  private static ValueSetExpansionException serverFailure(
      @Nonnull final BaseServerResponseException e) {
    return new ValueSetExpansionException(TerminologyServerReasons.httpFailure(e), e);
  }
}
