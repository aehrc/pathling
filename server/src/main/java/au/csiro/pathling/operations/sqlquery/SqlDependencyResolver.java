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

import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.views.FhirView;
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
 *   <li>prefers a request-supplied view whose URL matches;
 *   <li>otherwise searches stored {@code ViewDefinition}s by url, then {@code SQLView Library}s by
 *       url;
 *   <li>rejects a URL that matches both a ViewDefinition and a SQLView as ambiguous, and a URL that
 *       matches neither as not found - each naming the label and the reference.
 * </ol>
 *
 * <p>The resolution memoises by the resolved canonical key (the matched resource's url plus its
 * version, else the bare url), so a node referenced from more than one place (a diamond) -
 * including a bare-url reference and a {@code url|version} reference to the same stored resource -
 * is resolved once and shared. A reference encountered while it is already on the resolution stack
 * is a cycle and is rejected, as is a graph that nests deeper than the configured {@code
 * maxDependencyDepth}. All such failures are reported before any Spark execution.
 *
 * @author John Grimes
 */
@Component
public class SqlDependencyResolver {

  @Nonnull private final ViewResolver viewResolver;

  @Nonnull private final LibraryReferenceResolver libraryReferenceResolver;

  @Nonnull private final SqlLibraryParser libraryParser;

  @Nonnull private final ServerConfiguration serverConfiguration;

  /**
   * Constructs a new SqlDependencyResolver.
   *
   * @param viewResolver resolves ViewDefinition leaves by url, preferring request-supplied views
   * @param libraryReferenceResolver resolves a SQLView Library by canonical url from storage
   * @param libraryParser the shared parser for SQLView Libraries
   * @param serverConfiguration the server configuration (auth toggle and the dependency depth cap)
   */
  @Autowired
  public SqlDependencyResolver(
      @Nonnull final ViewResolver viewResolver,
      @Nonnull final LibraryReferenceResolver libraryReferenceResolver,
      @Nonnull final SqlLibraryParser libraryParser,
      @Nonnull final ServerConfiguration serverConfiguration) {
    this.viewResolver = viewResolver;
    this.libraryReferenceResolver = libraryReferenceResolver;
    this.libraryParser = libraryParser;
    this.serverConfiguration = serverConfiguration;
  }

  /**
   * Resolves the dependency graph for a parsed top-level query.
   *
   * @param topLevel the parsed top-level query (SQLQuery or SQLView)
   * @param suppliedViews request-supplied views keyed by the canonical URL they satisfy, used for
   *     the top-level query's direct references; nested SQLView dependencies resolve from storage
   *     only
   * @return the resolved dependency graph, topologically ordered
   * @throws InvalidRequestException if a reference is ambiguous, a cycle or depth-limit breach is
   *     detected, or a dependency is a malformed or wrong-typed resource
   * @throws ResourceNotFoundException if a reference matches no stored ViewDefinition or SQLView
   */
  @Nonnull
  public ResolvedDependencyGraph resolve(
      @Nonnull final ParsedSqlQuery topLevel, @Nonnull final Map<String, FhirView> suppliedViews) {
    final int maxDepth = serverConfiguration.getSqlQuery().getMaxDependencyDepth();
    final Map<String, ResolvedDependency> nodesByKey = new LinkedHashMap<>();
    final Set<String> resolutionStack = new LinkedHashSet<>();
    final Map<String, String> topLevelKeysByLabel =
        resolveReferences(
            topLevel.getViewReferences(), suppliedViews, 1, maxDepth, resolutionStack, nodesByKey);
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
      @Nonnull final Map<String, FhirView> suppliedViews,
      final int depth,
      final int maxDepth,
      @Nonnull final Set<String> resolutionStack,
      @Nonnull final Map<String, ResolvedDependency> nodesByKey) {
    final Map<String, String> keysByLabel = new LinkedHashMap<>();
    for (final ViewArtifactReference reference : references) {
      keysByLabel.put(
          reference.getLabel(),
          resolveReference(reference, suppliedViews, depth, maxDepth, resolutionStack, nodesByKey));
    }
    return keysByLabel;
  }

  /**
   * Resolves a single reference into the canonical key of its node, registering it if new. A
   * request-supplied view wins; otherwise the canonical url is matched against stored
   * ViewDefinitions then SQLView Libraries, rejecting an ambiguous match (both types) and a
   * not-found match (neither type).
   */
  @Nonnull
  private String resolveReference(
      @Nonnull final ViewArtifactReference reference,
      @Nonnull final Map<String, FhirView> suppliedViews,
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

    // A request-supplied view, matched by url, is preferred over storage.
    final Optional<ResolvedViewDefinition> suppliedView =
        viewResolver.resolveSuppliedView(reference, suppliedViews);
    if (suppliedView.isPresent()) {
      return registerLeaf(suppliedView.get(), nodesByKey);
    }

    // Search stored ViewDefinitions, then stored SQLView Libraries, both by url.
    final Optional<ResolvedViewDefinition> storedViewDefinition =
        viewResolver.resolveStoredViewDefinition(reference);
    final Optional<Library> sqlViewLibrary =
        libraryReferenceResolver.tryResolveSqlViewLibrary(reference.getCanonicalUrl());

    if (storedViewDefinition.isPresent() && sqlViewLibrary.isPresent()) {
      throw new InvalidRequestException(
          "The dependency for label '"
              + reference.getLabel()
              + "' (reference '"
              + reference.getCanonicalUrl()
              + "') is ambiguous: the canonical URL matches both a ViewDefinition and a SQLView");
    }
    if (storedViewDefinition.isPresent()) {
      return registerLeaf(storedViewDefinition.get(), nodesByKey);
    }
    if (sqlViewLibrary.isPresent()) {
      return resolveSqlView(
          sqlViewLibrary.get(), reference, depth, maxDepth, resolutionStack, nodesByKey);
    }
    throw new ResourceNotFoundException(
        "Failed to resolve the dependency for label '"
            + reference.getLabel()
            + "' with reference '"
            + reference.getCanonicalUrl()
            + "': no ViewDefinition or SQLView matches that canonical URL");
  }

  /** Registers a resolved ViewDefinition leaf (deduplicating diamonds) and returns its key. */
  @Nonnull
  private String registerLeaf(
      @Nonnull final ResolvedViewDefinition leaf,
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
  @Nonnull
  private String resolveSqlView(
      @Nonnull final Library library,
      @Nonnull final ViewArtifactReference reference,
      final int depth,
      final int maxDepth,
      @Nonnull final Set<String> resolutionStack,
      @Nonnull final Map<String, ResolvedDependency> nodesByKey) {
    final String canonicalKey = CanonicalReference.key(library.getUrl(), library.getVersion());

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
    // Nested dependencies resolve from storage only; request-supplied views satisfy the top-level
    // query's references, not the internals of a stored SQLView.
    final Map<String, String> childKeysByLabel =
        resolveReferences(
            parsed.getViewReferences(), Map.of(), depth + 1, maxDepth, resolutionStack, nodesByKey);
    resolutionStack.remove(canonicalKey);

    final ResolvedSqlView node =
        new ResolvedSqlView(canonicalKey, parsed.getSql(), childKeysByLabel);
    nodesByKey.put(canonicalKey, node);
    return canonicalKey;
  }
}
