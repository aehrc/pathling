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
import au.csiro.pathling.security.PathlingAuthority;
import au.csiro.pathling.security.ResourceAccess.AccessType;
import au.csiro.pathling.security.SecurityAspect;
import au.csiro.pathling.views.FhirView;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Library;
import org.hl7.fhir.r4.model.Reference;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Resolves the transitive dependency graph of a top-level query (a {@code SQLQuery} or a {@code
 * SQLView}) into a {@link ResolvedDependencyGraph}, without touching Spark. Each {@code
 * relatedArtifact} dependency is disambiguated, fetched, authorised, and parsed; {@code SQLView}
 * dependencies are recursed into so the full graph of virtual tables is resolved.
 *
 * <p>Reference disambiguation follows the SQL on FHIR resolution contract:
 *
 * <ul>
 *   <li>{@code ViewDefinition/[id]} resolves a {@code ViewDefinition} by logical id;
 *   <li>{@code Library/[id]} resolves a {@code SQLView} {@code Library} by logical id;
 *   <li>a bare canonical resolves a {@code ViewDefinition} first (by the canonical's final path
 *       segment as id), falling back to a {@code SQLView} {@code Library} by canonical url.
 * </ul>
 *
 * <p>The resolution memoises by canonical key, so a node referenced from more than one place (a
 * diamond) is resolved once and shared. A reference encountered while it is already on the
 * resolution stack is a cycle and is rejected, as is a graph that nests deeper than the configured
 * {@code maxDependencyDepth}. All such failures are reported before any Spark execution.
 *
 * @author John Grimes
 */
@Component
public class SqlDependencyResolver {

  private static final String VIEW_DEFINITION_PREFIX = "ViewDefinition/";

  private static final String LIBRARY_PREFIX = "Library/";

  private static final String LIBRARY = "Library";

  @Nonnull private final ViewResolver viewResolver;

  @Nonnull private final LibraryReferenceResolver libraryReferenceResolver;

  @Nonnull private final SqlLibraryParser libraryParser;

  @Nonnull private final ServerConfiguration serverConfiguration;

  /**
   * Constructs a new SqlDependencyResolver.
   *
   * @param viewResolver resolves ViewDefinition leaves, preferring request-supplied views
   * @param libraryReferenceResolver resolves a SQLView Library reference (relative or canonical)
   *     from storage
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
   * @param suppliedViews request-supplied views keyed by the ViewDefinition id they satisfy, used
   *     for the top-level query's direct references; nested SQLView dependencies resolve from
   *     storage only
   * @return the resolved dependency graph, topologically ordered
   * @throws InvalidRequestException if a reference is unresolvable, a cycle or depth-limit breach
   *     is detected, or a dependency is a malformed or wrong-typed resource
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
      @Nonnull final java.util.List<ViewArtifactReference> references,
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

  /** Resolves a single reference into the canonical key of its node, registering it if new. */
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

    final String referenceValue = reference.getCanonicalUrl();

    if (referenceValue.startsWith(VIEW_DEFINITION_PREFIX)) {
      // An explicit ViewDefinition reference is authoritative; no fallback.
      return registerLeaf(viewResolver.resolveViewDefinition(reference, suppliedViews), nodesByKey);
    }
    if (referenceValue.startsWith(LIBRARY_PREFIX)) {
      // An explicit Library reference is authoritative; resolve a SQLView, no fallback.
      return resolveSqlView(reference, depth, maxDepth, resolutionStack, nodesByKey);
    }

    // Bare canonical (or bare id): try a ViewDefinition first, then fall back to a SQLView.
    final Optional<ResolvedViewDefinition> viewDefinition =
        viewResolver.tryResolveViewDefinition(reference, suppliedViews);
    if (viewDefinition.isPresent()) {
      return registerLeaf(viewDefinition.get(), nodesByKey);
    }
    return resolveSqlView(reference, depth, maxDepth, resolutionStack, nodesByKey);
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
   * Resolves a reference to a {@code SQLView} {@code Library}, recursing into its own dependencies.
   * Detects diamonds (already resolved), cycles (currently on the resolution stack), and rejects a
   * {@code sql-query} Library referenced as a dependency.
   */
  @Nonnull
  private String resolveSqlView(
      @Nonnull final ViewArtifactReference reference,
      final int depth,
      final int maxDepth,
      @Nonnull final Set<String> resolutionStack,
      @Nonnull final Map<String, ResolvedDependency> nodesByKey) {
    final Library library = readSqlViewLibrary(reference);

    // The Library was read from storage: enforce the metadata READ check.
    if (serverConfiguration.getAuth().isEnabled()) {
      SecurityAspect.checkHasAuthority(PathlingAuthority.resourceAccess(AccessType.READ, LIBRARY));
    }

    final String canonicalKey = libraryCanonicalKey(library, reference);

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

  /**
   * Reads the SQLView Library named by the reference from storage, wrapping any resolution failure
   * in an actionable error that names the failing label and reference.
   */
  @Nonnull
  private Library readSqlViewLibrary(@Nonnull final ViewArtifactReference reference) {
    final IBaseResource resource;
    try {
      resource = libraryReferenceResolver.resolve(new Reference(reference.getCanonicalUrl()));
    } catch (final RuntimeException e) {
      throw new InvalidRequestException(
          "Failed to resolve the dependency for label '"
              + reference.getLabel()
              + "' with reference '"
              + reference.getCanonicalUrl()
              + "': "
              + e.getMessage());
    }
    if (resource instanceof final Library library) {
      return library;
    }
    throw new InvalidRequestException(
        "The dependency for label '"
            + reference.getLabel()
            + "' (reference '"
            + reference.getCanonicalUrl()
            + "') did not resolve to a Library");
  }

  /**
   * Computes the canonical key for a resolved SQLView Library: its type and logical id, so two
   * references to the same stored Library deduplicate. Falls back to the reference value when the
   * resolved resource carries no logical id.
   */
  @Nonnull
  private String libraryCanonicalKey(
      @Nonnull final Library library, @Nonnull final ViewArtifactReference reference) {
    final String id = library.getIdElement() == null ? null : library.getIdElement().getIdPart();
    return LIBRARY_PREFIX + (id != null && !id.isBlank() ? id : reference.getCanonicalUrl());
  }
}
