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

package au.csiro.pathling.operations.sqlquery;

import static au.csiro.pathling.operations.sql.SuppliedArtefacts.CONTEXT_EXPRESSION;
import static java.util.Objects.requireNonNullElse;

import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.config.TerminologyConfiguration;
import au.csiro.pathling.config.TerminologyMode;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.operations.sql.SqlOperationError;
import au.csiro.pathling.operations.sql.SuppliedArtefact;
import au.csiro.pathling.terminology.ImplicitTerminologyUrls;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.terminology.conceptmap.ConceptMapContent;
import au.csiro.pathling.terminology.conceptmap.ConceptMapContentException;
import au.csiro.pathling.terminology.conceptmap.ConceptMapLimitExceededException;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import ca.uhn.fhir.rest.server.exceptions.UnprocessableEntityException;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.ConceptMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Resolves a concept map dependency to its mappings, producing the {@link ResolvedConceptMap} leaf
 * the dependency graph registers. Mappings come from a ConceptMap supplied as {@code context},
 * converted directly, or otherwise from the server's configured terminology layer, whatever {@code
 * pathling.terminology} configures: a FHIR terminology server searched for ConceptMap resources by
 * canonical URL, or the local terminology store, which also synthesises the SNOMED CT implicit
 * concept maps.
 *
 * <p>The dependency resolver consults this collaborator for a canonical reference only once the
 * value set lookup has found nothing, or directly for a SNOMED CT implicit concept map URL, which
 * is never looked up as a value set, so it is the last source a canonical reference reaches. This
 * is the one place that applies the {@code pathling.sqlQuery.conceptMapMaxMappings} cap to a
 * concept map, that short-circuits when terminology is disabled, that raises the not-found for an
 * unresolved SNOMED CT implicit concept map URL, and that logs the provenance of every concept map
 * resolved.
 *
 * @author John Grimes
 */
@Slf4j
@Component
public class ConceptMapResolver {

  /** Names the local terminology store as a mapping source in the provenance line. */
  private static final String LOCAL_STORE_SOURCE = "local store";

  /** Stands in for an absent version in the provenance line. */
  private static final String NONE = "none";

  /**
   * Completes the not-found for a SNOMED CT implicit concept map URL in SERVER mode, where only the
   * terminology server's ConceptMap search is consulted.
   */
  private static final String LOCAL_MODE_ONLY_DETAIL =
      "SNOMED CT implicit concept maps are resolved only in local terminology mode";

  /** The terminology service, or null where terminology is disabled in configuration. */
  @Nullable private final TerminologyService terminologyService;

  /**
   * The largest number of mappings accepted, from {@code pathling.sqlQuery.conceptMapMaxMappings}.
   */
  private final int maxMappings;

  /** The mapping source named in the provenance line: the server URL or the local store. */
  @Nonnull private final String source;

  /** Whether terminology is enabled and resolved by a FHIR terminology server. */
  private final boolean serverMode;

  /**
   * Constructs a new ConceptMapResolver.
   *
   * @param pathlingContext the Pathling context, which supplies the terminology service
   * @param serverConfiguration the server configuration (the terminology settings and the mapping
   *     cap)
   */
  @Autowired
  public ConceptMapResolver(
      @Nonnull final PathlingContext pathlingContext,
      @Nonnull final ServerConfiguration serverConfiguration) {
    final TerminologyConfiguration terminology = serverConfiguration.getTerminology();
    this.terminologyService =
        terminology.isEnabled() ? pathlingContext.getTerminologyServiceFactory().build() : null;
    this.maxMappings = serverConfiguration.getSqlQuery().getConceptMapMaxMappings();
    final boolean localMode = TerminologyMode.LOCAL.equals(terminology.getMode());
    this.source = localMode ? LOCAL_STORE_SOURCE : terminology.getServerUrl();
    this.serverMode = terminology.isEnabled() && !localMode;
  }

  /**
   * Resolves a dependency's canonical reference to a concept map through the terminology layer,
   * reading the pinned version where the reference carries one and the latest otherwise.
   *
   * <p>The returned node is keyed by the reference canonical as written, not by the version the
   * terminology layer resolved, so that a second reference to the same string within a job reuses
   * it without a second lookup.
   *
   * <p>A SNOMED CT implicit concept map URL (a SNOMED CT base with a {@code fhir_cm} query) can be
   * nothing but a concept map, so where it is not resolved this raises the not-found itself rather
   * than returning empty. In SERVER mode the issue adds that such maps are resolved only in local
   * terminology mode, since only the local store synthesises them.
   *
   * @param reference the dependency reference, whose label names the relation in the SQL
   * @param canonical the parsed canonical of the reference
   * @return the resolved concept map, or empty where the terminology layer holds no concept map at
   *     the canonical or terminology is disabled
   * @throws ResourceNotFoundException if the URL is a SNOMED CT implicit concept map URL and no
   *     concept map is resolved for it
   */
  @Nonnull
  public Optional<ResolvedConceptMap> resolveCanonical(
      @Nonnull final ViewArtifactReference reference, @Nonnull final CanonicalReference canonical) {
    final String url = canonical.getUrl();
    final Optional<ResolvedConceptMap> resolved =
        terminologyService == null
            ? Optional.empty()
            : terminologyService
                .readConceptMap(url, canonical.getVersion(), maxMappings)
                .map(
                    content ->
                        resolved(
                            CanonicalReference.key(url, canonical.getVersion()), content, source));
    if (resolved.isEmpty() && ImplicitTerminologyUrls.isImplicitConceptMap(url)) {
      throw serverMode
          ? new ResourceNotFoundException(
              SqlDependencyResolver.dependencyFailure(
                  reference,
                  SqlDependencyResolver.NOT_FOUND_DETAIL + "; " + LOCAL_MODE_ONLY_DETAIL))
          : SqlDependencyResolver.notFound(reference);
    }
    return resolved;
  }

  /**
   * Resolves a dependency satisfied by a {@code context} ConceptMap to its mappings, converting the
   * resource directly without consulting the terminology layer, so that it resolves even where
   * terminology is disabled.
   *
   * <p>The returned node is keyed by the supplied resource's canonical ({@code url} plus its
   * version where it declares one), as a supplied ViewDefinition or ValueSet is.
   *
   * @param reference the dependency reference, whose label names the relation in the SQL
   * @param artefact the supplied entry, which must be a ConceptMap
   * @return the resolved concept map
   * @throws UnprocessableEntityException if the resource carries content the relation cannot
   *     represent, or more mappings than the configured maximum
   */
  @Nonnull
  public ResolvedConceptMap resolveSupplied(
      @Nonnull final ViewArtifactReference reference, @Nonnull final SuppliedArtefact artefact) {
    final ConceptMap conceptMap = artefact.getConceptMap();
    final String url = artefact.getUrl();
    final ConceptMapContent content;
    try {
      content = ConceptMapContent.fromResource(conceptMap, maxMappings);
    } catch (final ConceptMapLimitExceededException e) {
      throw limitExceeded(CONTEXT_EXPRESSION, reference, url, e);
    } catch (final ConceptMapContentException e) {
      throw unrepresentable(CONTEXT_EXPRESSION, reference, url, e.getMessage());
    }
    return resolved(
        CanonicalReference.key(url, artefact.getVersion()), content, CONTEXT_EXPRESSION);
  }

  /**
   * Names the concept map at fault in an issue: a {@code context} entry is the "supplied concept
   * map", so that a fault in what the request carried reads differently from one in what the
   * terminology layer returned for a canonical.
   *
   * @param expression the parameter at fault
   * @return the noun for the issue text, without its article
   */
  @Nonnull
  private static String subjectOf(@Nonnull final String expression) {
    return CONTEXT_EXPRESSION.equals(expression) ? "supplied concept map" : "concept map";
  }

  /**
   * Builds the {@code 422} for a concept map with more mappings than the configured maximum.
   *
   * @param expression the parameter at fault
   * @param reference the dependency reference
   * @param url the canonical URL of the concept map
   * @param cause the limit breach
   * @return the exception to throw
   */
  @Nonnull
  private static UnprocessableEntityException limitExceeded(
      @Nonnull final String expression,
      @Nonnull final ViewArtifactReference reference,
      @Nonnull final String url,
      @Nonnull final ConceptMapLimitExceededException cause) {
    return SqlOperationError.unprocessable(
        expression,
        "The "
            + subjectOf(expression)
            + " for label '"
            + reference.getLabel()
            + "' (canonical URL '"
            + url
            + "') has more than the maximum of "
            + cause.getLimit()
            + " mappings permitted by pathling.sqlQuery.conceptMapMaxMappings");
  }

  /**
   * Builds the {@code 422} for a concept map whose content the relation cannot represent, carrying
   * the reason.
   *
   * @param expression the parameter at fault
   * @param reference the dependency reference
   * @param url the canonical URL of the concept map
   * @param reason why the content cannot be represented
   * @return the exception to throw
   */
  @Nonnull
  private static UnprocessableEntityException unrepresentable(
      @Nonnull final String expression,
      @Nonnull final ViewArtifactReference reference,
      @Nonnull final String url,
      @Nonnull final String reason) {
    return SqlOperationError.unprocessable(
        expression,
        "The "
            + subjectOf(expression)
            + " for label '"
            + reference.getLabel()
            + "' (canonical URL '"
            + url
            + "') cannot be represented as a relation: "
            + reason);
  }

  /**
   * Builds the leaf for a concept map and logs its provenance: the canonical URL, the version
   * actually resolved, the source and the number of mappings.
   *
   * @param canonicalKey the reference canonical as written
   * @param content the mappings
   * @param source the source the mappings came from
   * @return the leaf
   */
  @Nonnull
  private static ResolvedConceptMap resolved(
      @Nonnull final String canonicalKey,
      @Nonnull final ConceptMapContent content,
      @Nonnull final String source) {
    log.info(
        "Resolved concept map '{}' (version {}) from {}: {} mappings",
        content.getUrl(),
        requireNonNullElse(content.getVersion(), NONE),
        source,
        content.getMappings().size());
    return new ResolvedConceptMap(canonicalKey, content);
  }
}
