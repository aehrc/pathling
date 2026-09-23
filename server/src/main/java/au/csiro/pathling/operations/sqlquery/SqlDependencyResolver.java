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

import au.csiro.pathling.config.ExternalTableConfiguration;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.operations.sql.SuppliedArtefact;
import au.csiro.pathling.operations.sql.SuppliedArtefacts;
import au.csiro.pathling.terminology.ImplicitTerminologyUrls;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.hl7.fhir.r4.model.Library;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Resolves the transitive dependency graph of a top-level query (a {@code SQLQuery} or a {@code
 * SQLView}) into a {@link ResolvedDependencyGraph}, without touching Spark. Each {@code
 * relatedArtifact} dependency is resolved by canonical URL, authorised, and parsed; {@code SQLView}
 * dependencies are recursed into so the full graph of virtual tables is resolved.
 *
 * <p>Reference resolution follows the SQL on FHIR canonical-reference contract: a {@code
 * relatedArtifact.resource} is an absolute canonical URL (optionally {@code |version}), matched
 * against the candidate resource's {@code url} - never its logical id. For a reference the
 * resolver:
 *
 * <ol>
 *   <li>prefers a request-supplied artefact whose URL matches: a ViewDefinition, a ValueSet or a
 *       ConceptMap is a leaf, and a SQLView is traversed in turn;
 *   <li>otherwise matches the bare URL (a reference carrying no version) against the external
 *       tables the operator has configured, and searches stored {@code ViewDefinition}s by url and
 *       {@code SQLView Library}s by url;
 *   <li>rejects a URL that matches more than one of those three sources as ambiguous;
 *   <li>otherwise, as a last resort, unless the URL's grammar marks it as a SNOMED CT implicit
 *       concept map (a SNOMED CT base with a {@code fhir_cm} query), asks the configured
 *       terminology layer to resolve the URL as a value set, whose membership becomes a five-column
 *       relation under the label;
 *   <li>otherwise, unless the URL's grammar marks it as an implicit value set (a SNOMED CT base
 *       with a {@code fhir_vs} query, or a {@code http://fhir.org/VCL} URL), asks the terminology
 *       layer to resolve it as a concept map, whose mappings become a nine-column relation under
 *       the label;
 *   <li>rejects a URL that matches nothing, the terminology layer included, as not found - each
 *       failure naming the label and the reference.
 * </ol>
 *
 * <p>The terminology layer is consulted last so that view dependencies never pay a terminology
 * round trip, and a stored or configured artefact sharing a value set's or a concept map's URL wins
 * without the collision being detected: that is an operator-side condition, not a request fault.
 * The value set lookup precedes the concept map lookup so that a value set that expands costs no
 * concept map request; a URL the terminology layer holds as both is a content error that is
 * likewise not detected, and the value set wins.
 *
 * <p>The resolution memoises by the resolved canonical key (the matched resource's url plus its
 * version, else the bare url), so a node referenced from more than one place (a diamond) -
 * including a bare-url reference and a {@code url|version} reference to the same stored resource -
 * is resolved once and shared. A value set or a concept map is keyed by the reference canonical as
 * written, so each distinct reference string in the job is looked up in the terminology layer once,
 * whichever kind it turns out to be. A reference encountered while it is already on the resolution
 * stack is a cycle and is rejected, as is a graph that nests deeper than the configured {@code
 * maxDependencyDepth}. All such failures are reported before any Spark execution.
 *
 * @author John Grimes
 */
@Component
public class SqlDependencyResolver {

  /**
   * The detail of the {@code 404} for a dependency that nothing matches, completing the sentence
   * {@link #dependencyFailure} begins.
   */
  static final String NOT_FOUND_DETAIL =
      "no ViewDefinition, SQLView, external table, concept map or value set matches that canonical"
          + " URL";

  @Nonnull private final ViewResolver viewResolver;

  @Nonnull private final LibraryReferenceResolver libraryReferenceResolver;

  @Nonnull private final ValueSetMembershipResolver valueSetMembershipResolver;

  @Nonnull private final ConceptMapResolver conceptMapResolver;

  @Nonnull private final SqlLibraryParser libraryParser;

  @Nonnull private final ServerConfiguration serverConfiguration;

  /** The operator-configured external tables, indexed by their canonical URL. */
  @Nonnull private final Map<String, ExternalTableConfiguration> externalTablesByUrl;

  /**
   * Constructs a new SqlDependencyResolver.
   *
   * @param viewResolver resolves ViewDefinition leaves by url from storage
   * @param libraryReferenceResolver resolves a SQLView Library by canonical url from storage
   * @param valueSetMembershipResolver resolves a value set leaf from a supplied ValueSet or through
   *     the terminology layer
   * @param conceptMapResolver resolves a concept map leaf from a supplied ConceptMap or through the
   *     terminology layer
   * @param libraryParser the shared parser for SQLView Libraries
   * @param serverConfiguration the server configuration (auth toggle, the dependency depth cap and
   *     the configured external tables)
   */
  @Autowired
  public SqlDependencyResolver(
      @Nonnull final ViewResolver viewResolver,
      @Nonnull final LibraryReferenceResolver libraryReferenceResolver,
      @Nonnull final ValueSetMembershipResolver valueSetMembershipResolver,
      @Nonnull final ConceptMapResolver conceptMapResolver,
      @Nonnull final SqlLibraryParser libraryParser,
      @Nonnull final ServerConfiguration serverConfiguration) {
    this.viewResolver = viewResolver;
    this.libraryReferenceResolver = libraryReferenceResolver;
    this.valueSetMembershipResolver = valueSetMembershipResolver;
    this.conceptMapResolver = conceptMapResolver;
    this.libraryParser = libraryParser;
    this.serverConfiguration = serverConfiguration;
    // URL uniqueness is enforced by Bean Validation at bind time, so the keys cannot collide.
    this.externalTablesByUrl =
        serverConfiguration.getSqlQuery().getExternalTables().stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    ExternalTableConfiguration::getUrl, Function.identity()));
  }

  /**
   * Resolves the dependency graph for a parsed top-level query, memoising within this call only.
   *
   * @param topLevel the parsed top-level query (SQLQuery or SQLView)
   * @param supplied request-supplied artefacts matched by the canonical URL they satisfy
   * @return the resolved dependency graph, topologically ordered
   * @throws InvalidRequestException if a reference is ambiguous, a cycle or depth-limit breach is
   *     detected, or a dependency is a malformed or wrong-typed resource
   * @throws ResourceNotFoundException if a reference matches no ViewDefinition, SQLView, external
   *     table, concept map or value set
   * @throws ca.uhn.fhir.rest.server.exceptions.UnprocessableEntityException if a value set,
   *     supplied or resolved, has a membership that cannot be determined or exceeds the configured
   *     maximum, or a supplied concept map carries content the relation cannot represent or more
   *     mappings than the configured maximum
   */
  @Nonnull
  public ResolvedDependencyGraph resolve(
      @Nonnull final ParsedSqlQuery topLevel, @Nonnull final SuppliedArtefacts supplied) {
    return resolve(topLevel, supplied, new LinkedHashMap<>());
  }

  /**
   * Resolves the dependency graph for a parsed top-level query, memoising into a caller-supplied
   * node map.
   *
   * <p>Passing one map across every subject of an export job gives the contract's
   * one-resolution-per-canonical-URL guarantee: a dependency shared by two subjects is resolved
   * once and both subjects see the same artefact.
   *
   * @param topLevel the parsed top-level query (SQLQuery or SQLView)
   * @param supplied request-supplied artefacts matched by the canonical URL they satisfy
   * @param nodesByKey the memoisation map, shared across the subjects of one job
   * @return the resolved dependency graph, topologically ordered
   * @throws InvalidRequestException if a reference is ambiguous, a cycle or depth-limit breach is
   *     detected, or a dependency is a malformed or wrong-typed resource
   * @throws ResourceNotFoundException if a reference matches no ViewDefinition, SQLView, external
   *     table, concept map or value set
   * @throws ca.uhn.fhir.rest.server.exceptions.UnprocessableEntityException if a value set,
   *     supplied or resolved, has a membership that cannot be determined or exceeds the configured
   *     maximum, or a supplied concept map carries content the relation cannot represent or more
   *     mappings than the configured maximum
   */
  @Nonnull
  public ResolvedDependencyGraph resolve(
      @Nonnull final ParsedSqlQuery topLevel,
      @Nonnull final SuppliedArtefacts supplied,
      @Nonnull final Map<String, ResolvedDependency> nodesByKey) {
    final int maxDepth = serverConfiguration.getSqlQuery().getMaxDependencyDepth();
    final Set<String> resolutionStack = new LinkedHashSet<>();
    final Map<String, String> topLevelKeysByLabel =
        resolveReferences(
            topLevel.getViewReferences(), supplied, 1, maxDepth, resolutionStack, nodesByKey);
    return new ResolvedDependencyGraph(
        new ArrayList<>(nodesByKey.values()), topLevelKeysByLabel, nodesByKey);
  }

  /**
   * Resolves a list of references in order, returning their labels mapped to the canonical keys of
   * the nodes they resolve to.
   */
  @Nonnull
  private Map<String, String> resolveReferences(
      @Nonnull final List<ViewArtifactReference> references,
      @Nonnull final SuppliedArtefacts supplied,
      final int depth,
      final int maxDepth,
      @Nonnull final Set<String> resolutionStack,
      @Nonnull final Map<String, ResolvedDependency> nodesByKey) {
    final Map<String, String> keysByLabel = new LinkedHashMap<>();
    for (final ViewArtifactReference reference : references) {
      keysByLabel.put(
          reference.getLabel(),
          resolveReference(reference, supplied, depth, maxDepth, resolutionStack, nodesByKey));
    }
    return keysByLabel;
  }

  /**
   * Resolves a single reference into the canonical key of its node, registering it if new. A
   * request-supplied artefact wins, whether a ViewDefinition, a SQLView, a ValueSet or a
   * ConceptMap; otherwise the canonical url is matched against the configured external tables (only
   * when the reference carries no version), stored ViewDefinitions and SQLView Libraries, rejecting
   * an ambiguous match (more than one source). A reference matching none of those is passed to the
   * terminology layer as a value set, unless its URL is a SNOMED CT implicit concept map, and then,
   * unless its URL is an implicit value set, as a concept map, and is not found only when those too
   * yield nothing.
   */
  @Nonnull
  private String resolveReference(
      @Nonnull final ViewArtifactReference reference,
      @Nonnull final SuppliedArtefacts supplied,
      final int depth,
      final int maxDepth,
      @Nonnull final Set<String> resolutionStack,
      @Nonnull final Map<String, ResolvedDependency> nodesByKey) {
    if (depth > maxDepth) {
      throw new InvalidRequestException(
          "Dependency graph nests deeper than the configured maximum of "
              + maxDepth
              + " (at label '"
              + reference.getLabel()
              + "', reference '"
              + reference.getCanonicalUrl()
              + "')");
    }

    // A request-supplied artefact, matched by url and agreeing version, outranks storage. A
    // supplied SQLView is traversed in turn, so a chain of supplied artefacts resolves. A supplied
    // ValueSet or ConceptMap is a leaf; a node already registered under its key is reused, so a
    // compose-only ValueSet reached under two labels is expanded once, and a ConceptMap reached
    // under two labels is converted once.
    final CanonicalReference canonical = CanonicalReference.parse(reference.getCanonicalUrl());
    final Optional<SuppliedArtefact> suppliedArtefact =
        supplied.match(canonical.getUrl(), canonical.getVersion());
    if (suppliedArtefact.isPresent()) {
      final SuppliedArtefact artefact = suppliedArtefact.get();
      final String suppliedKey = CanonicalReference.key(artefact.getUrl(), artefact.getVersion());
      if (artefact.isView()) {
        return registerLeaf(
            new ResolvedViewDefinition(suppliedKey, artefact.getView()), nodesByKey);
      }
      if (artefact.isValueSet()) {
        if (nodesByKey.containsKey(suppliedKey)) {
          return suppliedKey;
        }
        return registerLeaf(
            valueSetMembershipResolver.resolveSupplied(reference, artefact), nodesByKey);
      }
      if (artefact.isConceptMap()) {
        if (nodesByKey.containsKey(suppliedKey)) {
          return suppliedKey;
        }
        return registerLeaf(conceptMapResolver.resolveSupplied(reference, artefact), nodesByKey);
      }
      return resolveSqlView(
          artefact.getSqlView(),
          reference,
          suppliedKey,
          supplied,
          depth,
          maxDepth,
          resolutionStack,
          nodesByKey);
    }

    // A configured external table matches the bare url only: tables have no version, so a pinned
    // reference can never mean one. Storage is still searched so that a URL bound to a table and
    // also stored as an artefact is reported as ambiguous rather than one side silently winning.
    // The stored lookups enforce their metadata read checks only once they find a match.
    final ExternalTableConfiguration externalTable =
        canonical.getVersion() == null ? externalTablesByUrl.get(canonical.getUrl()) : null;
    final Optional<ResolvedViewDefinition> storedViewDefinition =
        viewResolver.resolveStoredViewDefinition(reference);
    final Optional<Library> sqlViewLibrary =
        libraryReferenceResolver.tryResolveSqlViewLibrary(reference.getCanonicalUrl());

    final List<String> matchedKinds = new ArrayList<>(3);
    if (externalTable != null) {
      matchedKinds.add("an external table");
    }
    if (storedViewDefinition.isPresent()) {
      matchedKinds.add("a ViewDefinition");
    }
    if (sqlViewLibrary.isPresent()) {
      matchedKinds.add("a SQLView");
    }
    if (matchedKinds.size() > 1) {
      throw new InvalidRequestException(
          "The dependency for label '"
              + reference.getLabel()
              + "' (reference '"
              + reference.getCanonicalUrl()
              + "') is ambiguous: the canonical URL matches "
              + describeKinds(matchedKinds));
    }
    if (externalTable != null) {
      return registerLeaf(
          new ResolvedExternalTable(
              externalTable.getUrl(), externalTable.getPath(), externalTable.getFormat()),
          nodesByKey);
    }
    if (storedViewDefinition.isPresent()) {
      return registerLeaf(storedViewDefinition.get(), nodesByKey);
    }
    if (sqlViewLibrary.isPresent()) {
      final Library library = sqlViewLibrary.get();
      return resolveSqlView(
          library,
          reference,
          CanonicalReference.key(library.getUrl(), library.getVersion()),
          supplied,
          depth,
          maxDepth,
          resolutionStack,
          nodesByKey);
    }

    // Nothing supplied, configured or stored matches, so the reference is a value set or a concept
    // map if anything. A node already reached under the same reference string is reused, so the
    // terminology layer is consulted once per distinct reference in the job, whichever kind the
    // reference turns out to be, even though the stored lookups above run again for each
    // occurrence.
    final String terminologyKey =
        CanonicalReference.key(canonical.getUrl(), canonical.getVersion());
    if (nodesByKey.containsKey(terminologyKey)) {
      return terminologyKey;
    }
    // A SNOMED CT implicit concept map URL names a concept map by its grammar, so it cannot be a
    // value set and the value set lookup would be wasted; the concept map lookup raises the
    // not-found for it itself.
    if (!ImplicitTerminologyUrls.isImplicitConceptMap(canonical.getUrl())) {
      final Optional<ResolvedValueSet> valueSet =
          valueSetMembershipResolver.resolveCanonical(reference, canonical);
      if (valueSet.isPresent()) {
        return registerLeaf(valueSet.get(), nodesByKey);
      }
    }
    // An implicit value set URL names a value set by its grammar, so it cannot be a concept map and
    // the concept map lookup would be wasted.
    if (!ImplicitTerminologyUrls.isImplicitValueSet(canonical.getUrl())) {
      final Optional<ResolvedConceptMap> conceptMap =
          conceptMapResolver.resolveCanonical(reference, canonical);
      if (conceptMap.isPresent()) {
        return registerLeaf(conceptMap.get(), nodesByKey);
      }
    }
    throw notFound(reference);
  }

  /**
   * Builds the text of an issue about a dependency that could not be resolved: the prefix naming
   * the label and the reference that every such issue shares, followed by the given detail.
   *
   * @param reference the dependency reference
   * @param detail what went wrong, as a clause completing the sentence
   * @return the issue text
   */
  @Nonnull
  static String dependencyFailure(
      @Nonnull final ViewArtifactReference reference, @Nonnull final String detail) {
    return "Failed to resolve the dependency for label '"
        + reference.getLabel()
        + "' with reference '"
        + reference.getCanonicalUrl()
        + "': "
        + detail;
  }

  /**
   * Builds the {@code 404} for a dependency that nothing matches, the terminology layer included.
   *
   * @param reference the dependency reference
   * @return the exception to throw
   */
  @Nonnull
  static ResourceNotFoundException notFound(@Nonnull final ViewArtifactReference reference) {
    return new ResourceNotFoundException(dependencyFailure(reference, NOT_FOUND_DETAIL));
  }

  /** Joins two or more matched kinds into prose: "both X and Y" for two, "X, Y and Z" for three. */
  @Nonnull
  private static String describeKinds(@Nonnull final List<String> kinds) {
    final String last = kinds.get(kinds.size() - 1);
    final String head = String.join(", ", kinds.subList(0, kinds.size() - 1));
    return (kinds.size() == 2 ? "both " : "") + head + " and " + last;
  }

  /**
   * Registers a resolved leaf (a ViewDefinition, an external table, a value set or a concept map),
   * deduplicating diamonds, and returns its key.
   */
  @Nonnull
  private String registerLeaf(
      @Nonnull final ResolvedDependency leaf,
      @Nonnull final Map<String, ResolvedDependency> nodesByKey) {
    nodesByKey.putIfAbsent(leaf.getCanonicalKey(), leaf);
    return leaf.getCanonicalKey();
  }

  /**
   * Resolves a matched {@code SQLView} {@code Library}, recursing into its own dependencies. Keys
   * the node by the resolved canonical (the library's url plus its version, else the bare url), so
   * two references to the same stored Library - including a bare-url and a {@code url|version}
   * reference - deduplicate. Detects diamonds (already resolved), cycles (currently on the
   * resolution stack), and rejects a {@code sql-query} Library referenced as a dependency.
   */
  @SuppressWarnings("java:S107")
  @Nonnull
  private String resolveSqlView(
      @Nonnull final Library library,
      @Nonnull final ViewArtifactReference reference,
      @Nonnull final String canonicalKey,
      @Nonnull final SuppliedArtefacts supplied,
      final int depth,
      final int maxDepth,
      @Nonnull final Set<String> resolutionStack,
      @Nonnull final Map<String, ResolvedDependency> nodesByKey) {

    // A node already fully resolved is shared (diamond dedup).
    if (nodesByKey.containsKey(canonicalKey)) {
      return canonicalKey;
    }
    // A node still being resolved is a cycle.
    if (resolutionStack.contains(canonicalKey)) {
      throw new InvalidRequestException(
          "Cyclic dependency detected: "
              + String.join(" -> ", resolutionStack)
              + " -> "
              + canonicalKey);
    }

    final ParsedSqlQuery parsed = libraryParser.parse(library);
    if (!parsed.isView()) {
      throw new InvalidRequestException(
          "The dependency for label '"
              + reference.getLabel()
              + "' (reference '"
              + reference.getCanonicalUrl()
              + "') is a "
              + parsed.getLibraryTypeCode()
              + " Library, but only a SQLView may be referenced as a dependency");
    }

    resolutionStack.add(canonicalKey);
    // Supplied artefacts are matched at every level, so a dependency reachable only through a
    // supplied SQLView is still satisfied by another supplied entry.
    final Map<String, String> childKeysByLabel =
        resolveReferences(
            parsed.getViewReferences(), supplied, depth + 1, maxDepth, resolutionStack, nodesByKey);
    resolutionStack.remove(canonicalKey);

    final ResolvedSqlView node =
        new ResolvedSqlView(canonicalKey, parsed.getSql(), childKeysByLabel);
    nodesByKey.put(canonicalKey, node);
    return canonicalKey;
  }
}
