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

package au.csiro.pathling.terminology.conceptmap;

import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.terminology.TerminologyServerReasons;
import au.csiro.pathling.terminology.local.AmbiguousVersionException;
import au.csiro.pathling.terminology.local.VersionResolver;
import ca.uhn.fhir.rest.client.exceptions.FhirClientConnectionException;
import ca.uhn.fhir.rest.server.exceptions.BaseServerResponseException;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.r4.model.ConceptMap;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.UriType;

/**
 * Reads a concept map from a FHIR terminology server in three steps: a summary search of ConceptMap
 * resources by canonical URL (and version, where pinned), following {@code next} links and keeping
 * the entries whose {@code url} equals the request exactly; a choice among the entries, taking the
 * one entry a pinned reference matches and the latest of several an unpinned reference matches; and
 * one read of the chosen resource, converted with {@link ConceptMapContent#fromResource}. No other
 * request is made, so the content of a version that is not chosen is never fetched.
 *
 * <p>A search answered with any {@code 4xx} status or {@code 501} means the server holds no concept
 * map at the URL, or does not search them, and is reported as empty. Any other {@code 5xx}, or a
 * server that cannot be reached, leaves it unknown whether the URL names a concept map at all and
 * is reported as a {@link ConceptMapLookupException}. A failure of the read, once a map has been
 * chosen, is a fault in a found map and is reported as a {@link ConceptMapContentException}.
 *
 * @author John Grimes
 */
public class ConceptMapReader {

  /** The HTTP status that reports an operation the server does not implement. */
  private static final int NOT_IMPLEMENTED = 501;

  /** The lowest HTTP status that reports a server error. */
  private static final int SERVER_ERROR = 500;

  @Nonnull private final TerminologyClient terminologyClient;
  @Nonnull private final VersionResolver versionResolver;

  /**
   * Creates a reader over a terminology client.
   *
   * @param terminologyClient the client for communicating with the terminology server
   * @param versionResolver the resolver that selects the latest of several versions
   */
  public ConceptMapReader(
      @Nonnull final TerminologyClient terminologyClient,
      @Nonnull final VersionResolver versionResolver) {
    this.terminologyClient = terminologyClient;
    this.versionResolver = versionResolver;
  }

  /**
   * Reads a concept map by canonical URL.
   *
   * @param url the canonical URL of the concept map, without a version suffix
   * @param version the version to read, or null for the latest the server holds
   * @param maxMappings the largest number of rows the caller will accept
   * @return the content, or empty where the server holds no concept map at the URL
   * @throws ConceptMapLookupException if the search fails with a server error other than 501, or
   *     the server cannot be reached
   * @throws ConceptMapVersionException if the version to use cannot be determined
   * @throws ConceptMapContentException if the chosen map cannot be read, or its content cannot be
   *     represented
   * @throws ConceptMapLimitExceededException if the map has more than {@code maxMappings} rows
   */
  @Nonnull
  public Optional<ConceptMapContent> read(
      @Nonnull final String url, @Nullable final String version, final int maxMappings) {
    final List<ConceptMap> candidates = search(url, version);
    if (candidates.isEmpty()) {
      return Optional.empty();
    }
    final ConceptMap chosen = choose(candidates, url, version);
    return Optional.of(ConceptMapContent.fromResource(fetch(chosen), maxMappings));
  }

  /**
   * Searches for summaries of the ConceptMap resources at a URL, following every {@code next} link.
   *
   * @return the summaries whose {@code url} equals the request exactly, in the order the server
   *     returned them
   */
  @Nonnull
  private List<ConceptMap> search(@Nonnull final String url, @Nullable final String version) {
    final List<ConceptMap> matches = new ArrayList<>();
    try {
      Bundle page =
          terminologyClient.searchConceptMaps(
              new UriType(url), version == null ? null : new StringType(version));
      while (true) {
        for (final BundleEntryComponent entry : page.getEntry()) {
          if (entry.getResource() instanceof final ConceptMap conceptMap
              && url.equals(conceptMap.getUrl())) {
            matches.add(conceptMap);
          }
        }
        if (page.getLink(Bundle.LINK_NEXT) == null) {
          return matches;
        }
        page = terminologyClient.nextPage(page);
      }
    } catch (final FhirClientConnectionException e) {
      throw new ConceptMapLookupException(
          TerminologyServerReasons.unreachable(terminologyClient.getServerUrl()), e);
    } catch (final BaseServerResponseException e) {
      if (e.getStatusCode() < SERVER_ERROR || e.getStatusCode() == NOT_IMPLEMENTED) {
        // The server holds no concept map at the URL, or does not search concept maps at all.
        return List.of();
      }
      throw new ConceptMapLookupException(TerminologyServerReasons.httpFailure(e), e);
    }
  }

  /**
   * Chooses the summary to read: the one a pinned reference matches, or the latest of those an
   * unpinned reference matches.
   */
  @Nonnull
  private ConceptMap choose(
      @Nonnull final List<ConceptMap> candidates,
      @Nonnull final String url,
      @Nullable final String version) {
    if (candidates.size() == 1) {
      return candidates.get(0);
    }
    if (version != null) {
      throw new ConceptMapVersionException(
          "found "
              + candidates.size()
              + " ConceptMaps with the URL "
              + url
              + " and version "
              + version);
    }
    try {
      final ConceptMap latest =
          versionResolver.getLatestOfVersions(
              candidates, candidate -> candidate.hasVersion() ? candidate.getVersion() : null, url);
      if (latest == null) {
        throw new ConceptMapVersionException(
            "unable to determine the latest version of the ConceptMaps with the URL " + url);
      }
      return latest;
    } catch (final AmbiguousVersionException e) {
      throw new ConceptMapVersionException(e.getMessage(), e);
    }
  }

  /** Reads the full resource of a chosen summary. */
  @Nonnull
  private ConceptMap fetch(@Nonnull final ConceptMap chosen) {
    final String id = chosen.getIdElement().getIdPart();
    if (id == null) {
      throw new ConceptMapContentException(
          "the terminology server returned a ConceptMap with no id for the URL " + chosen.getUrl());
    }
    try {
      return terminologyClient.readConceptMap(new IdType(id));
    } catch (final FhirClientConnectionException e) {
      throw new ConceptMapContentException(
          TerminologyServerReasons.unreachable(terminologyClient.getServerUrl()), e);
    } catch (final BaseServerResponseException e) {
      throw new ConceptMapContentException(TerminologyServerReasons.httpFailure(e), e);
    }
  }
}
