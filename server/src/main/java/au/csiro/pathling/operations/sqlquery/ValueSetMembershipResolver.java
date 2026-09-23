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
import au.csiro.pathling.operations.sql.SubjectResolver;
import au.csiro.pathling.operations.sql.SuppliedArtefact;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.terminology.expand.ExpansionLimitExceededException;
import au.csiro.pathling.terminology.expand.ValueSetExpansion;
import au.csiro.pathling.terminology.expand.ValueSetExpansionException;
import ca.uhn.fhir.rest.server.exceptions.UnprocessableEntityException;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.ValueSet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Resolves a value set dependency to its membership, producing the {@link ResolvedValueSet} leaf
 * the dependency graph registers. Membership comes from a {@code context} ValueSet where the
 * request supplies one - its expansion used as-is, or its compose expanded - and otherwise from the
 * server's configured terminology layer, whatever {@code pathling.terminology} configures: a FHIR
 * terminology server expanded through {@code $expand}, or the local terminology store.
 *
 * <p>This is the one place that separates a value set that cannot be resolved (an empty result,
 * which the dependency resolver reports as not found) from one that resolves but whose membership
 * cannot be determined (a {@code 422} naming the label, the canonical URL and the reason), that
 * applies the {@code pathling.sqlQuery.valueSetMaxMembers} cap, that short-circuits when
 * terminology is disabled, and that logs the provenance of every membership resolved.
 *
 * @author John Grimes
 */
@Slf4j
@Component
public class ValueSetMembershipResolver {

  /** Names the local terminology store as a membership source in the provenance line. */
  private static final String LOCAL_STORE_SOURCE = "local store";

  /** Stands in for an absent value in the provenance line. */
  private static final String NONE = "none";

  /** The terminology service, or null where terminology is disabled in configuration. */
  @Nullable private final TerminologyService terminologyService;

  /** The largest membership accepted, from {@code pathling.sqlQuery.valueSetMaxMembers}. */
  private final int maxMembers;

  /** The membership source named in the provenance line: the server URL or the local store. */
  @Nonnull private final String source;

  /**
   * Constructs a new ValueSetMembershipResolver.
   *
   * @param pathlingContext the Pathling context, which supplies the terminology service
   * @param serverConfiguration the server configuration (the terminology settings and the
   *     membership cap)
   */
  @Autowired
  public ValueSetMembershipResolver(
      @Nonnull final PathlingContext pathlingContext,
      @Nonnull final ServerConfiguration serverConfiguration) {
    final TerminologyConfiguration terminology = serverConfiguration.getTerminology();
    this.terminologyService =
        terminology.isEnabled() ? pathlingContext.getTerminologyServiceFactory().build() : null;
    this.maxMembers = serverConfiguration.getSqlQuery().getValueSetMaxMembers();
    this.source =
        TerminologyMode.LOCAL.equals(terminology.getMode())
            ? LOCAL_STORE_SOURCE
            : terminology.getServerUrl();
  }

  /**
   * Resolves a dependency's canonical reference to a value set through the terminology layer,
   * expanding it with the pinned version where the reference carries one.
   *
   * <p>The returned node is keyed by the reference canonical as written, not by the version the
   * terminology layer resolved, so that a second reference to the same string within a job reuses
   * it without a second expansion.
   *
   * @param reference the dependency reference, whose label names the relation in the SQL
   * @param canonical the parsed canonical of the reference
   * @return the resolved value set, or empty where the terminology layer cannot resolve the
   *     canonical or terminology is disabled
   * @throws UnprocessableEntityException if the value set resolves but its membership cannot be
   *     determined, or exceeds the configured maximum number of members
   */
  @Nonnull
  public Optional<ResolvedValueSet> resolveCanonical(
      @Nonnull final ViewArtifactReference reference, @Nonnull final CanonicalReference canonical) {
    if (terminologyService == null) {
      return Optional.empty();
    }
    final String url = canonical.getUrl();
    final Optional<ValueSetExpansion> expansion;
    try {
      expansion = terminologyService.expand(url, canonical.getVersion(), maxMembers);
    } catch (final ExpansionLimitExceededException e) {
      throw limitExceeded(SubjectResolver.SUBJECT_EXPRESSION, reference, url, e);
    } catch (final ValueSetExpansionException e) {
      throw undeterminable(SubjectResolver.SUBJECT_EXPRESSION, reference, url, e.getMessage());
    }
    return expansion.map(
        resolved ->
            resolved(CanonicalReference.key(url, canonical.getVersion()), resolved, source));
  }

  /**
   * Resolves a dependency satisfied by a {@code context} ValueSet to its membership. A resource
   * carrying an {@code expansion} supplies the membership as-is, without consulting the terminology
   * layer; one carrying only a {@code compose} is expanded by the terminology layer; one carrying
   * neither defines no membership.
   *
   * <p>The returned node is keyed by the supplied resource's canonical ({@code url} plus its
   * version where it declares one), as a supplied ViewDefinition is.
   *
   * @param reference the dependency reference, whose label names the relation in the SQL
   * @param artefact the supplied entry, which must be a ValueSet
   * @return the resolved value set
   * @throws UnprocessableEntityException if the resource defines no membership, its expansion is
   *     incomplete or has an entry that does not identify a member, its compose cannot be expanded
   *     (including when terminology is disabled), or the membership exceeds the configured maximum
   *     number of members
   */
  @Nonnull
  public ResolvedValueSet resolveSupplied(
      @Nonnull final ViewArtifactReference reference, @Nonnull final SuppliedArtefact artefact) {
    final ValueSet valueSet = artefact.getValueSet();
    final String url = artefact.getUrl();
    final ValueSetExpansion expansion;
    final String provenance;
    try {
      if (valueSet.hasExpansion()) {
        expansion = ValueSetExpansion.fromResource(valueSet, maxMembers);
        provenance = CONTEXT_EXPRESSION;
      } else if (!valueSet.hasCompose()) {
        throw SqlOperationError.unprocessable(
            CONTEXT_EXPRESSION,
            "The "
                + subjectOf(CONTEXT_EXPRESSION)
                + " for label '"
                + reference.getLabel()
                + "' (canonical URL '"
                + url
                + "') defines no membership: it carries neither an expansion nor a compose");
      } else if (terminologyService == null) {
        throw undeterminable(
            CONTEXT_EXPRESSION,
            reference,
            url,
            "the value set carries only a compose and terminology is disabled");
      } else {
        expansion = terminologyService.expand(valueSet, maxMembers);
        provenance = CONTEXT_EXPRESSION + ", expanded by " + source;
      }
    } catch (final ExpansionLimitExceededException e) {
      throw limitExceeded(CONTEXT_EXPRESSION, reference, url, e);
    } catch (final ValueSetExpansionException e) {
      throw undeterminable(CONTEXT_EXPRESSION, reference, url, e.getMessage());
    }
    return resolved(CanonicalReference.key(url, artefact.getVersion()), expansion, provenance);
  }

  /**
   * Builds the leaf for a membership and logs its provenance: the canonical URL and the version
   * actually resolved, the source, the expansion identifier and timestamp where the source records
   * them, the code system versions, and the member count.
   *
   * @param canonicalKey the reference canonical as written
   * @param expansion the membership
   * @param source the source the membership came from
   * @return the leaf
   */
  @Nonnull
  private static ResolvedValueSet resolved(
      @Nonnull final String canonicalKey,
      @Nonnull final ValueSetExpansion expansion,
      @Nonnull final String source) {
    log.info(
        "Resolved value set '{}' (version {}) from {}: {} members; expansion {} at {}; code"
            + " systems {}",
        expansion.getUrl(),
        requireNonNullElse(expansion.getVersion(), NONE),
        source,
        expansion.getMembers().size(),
        requireNonNullElse(expansion.getIdentifier(), NONE),
        requireNonNullElse(expansion.getTimestamp(), NONE),
        expansion.getCodeSystemVersions());
    return new ResolvedValueSet(canonicalKey, expansion);
  }

  /**
   * Names the value set at fault in an issue: a {@code context} entry is the "supplied value set",
   * so that a fault in what the request carried reads differently from one in what the terminology
   * layer returned for a canonical.
   *
   * @param expression the parameter at fault
   * @return the noun for the issue text, without its article
   */
  @Nonnull
  private static String subjectOf(@Nonnull final String expression) {
    return CONTEXT_EXPRESSION.equals(expression) ? "supplied value set" : "value set";
  }

  /**
   * Builds the {@code 422} for a value set whose membership exceeds the configured maximum.
   *
   * @param expression the parameter at fault
   * @param reference the dependency reference
   * @param url the canonical URL of the value set
   * @param cause the limit breach
   * @return the exception to throw
   */
  @Nonnull
  private static UnprocessableEntityException limitExceeded(
      @Nonnull final String expression,
      @Nonnull final ViewArtifactReference reference,
      @Nonnull final String url,
      @Nonnull final ExpansionLimitExceededException cause) {
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
            + " members permitted by pathling.sqlQuery.valueSetMaxMembers");
  }

  /**
   * Builds the {@code 422} for a value set that resolves but whose membership cannot be determined,
   * carrying the reason. The reason is relayed exactly as the terminology layer gave it, which for
   * an unreachable server names its configured URL and nothing else.
   *
   * @param expression the parameter at fault
   * @param reference the dependency reference
   * @param url the canonical URL of the value set
   * @param reason why the membership could not be determined
   * @return the exception to throw
   */
  @Nonnull
  private static UnprocessableEntityException undeterminable(
      @Nonnull final String expression,
      @Nonnull final ViewArtifactReference reference,
      @Nonnull final String url,
      @Nonnull final String reason) {
    return SqlOperationError.unprocessable(
        expression,
        "The membership of the "
            + subjectOf(expression)
            + " for label '"
            + reference.getLabel()
            + "' (canonical URL '"
            + url
            + "') could not be determined: "
            + reason);
  }
}
