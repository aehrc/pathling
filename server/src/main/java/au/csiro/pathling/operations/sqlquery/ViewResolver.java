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
import au.csiro.pathling.errors.ResourceNotFoundError;
import au.csiro.pathling.read.ReadExecutor;
import au.csiro.pathling.security.PathlingAuthority;
import au.csiro.pathling.security.ResourceAccess.AccessType;
import au.csiro.pathling.security.SecurityAspect;
import au.csiro.pathling.views.FhirView;
import au.csiro.pathling.views.ViewDefinitionGson;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import jakarta.annotation.Nonnull;
import java.util.Map;
import java.util.Optional;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Resolves a single {@link ViewArtifactReference} that targets a {@code ViewDefinition} into a
 * {@link ResolvedViewDefinition} leaf node, preferring a request-supplied view over server storage
 * and performing the per-projected-resource-type authorisation check along the way. Has no Spark
 * dependency; the {@link FhirView} is the parsed view configuration, not yet a {@code Dataset}.
 *
 * <p>Used by {@link SqlDependencyResolver} for the {@code ViewDefinition} leaves of a dependency
 * graph, both for explicit {@code ViewDefinition/[id]} references and as the first attempt when
 * disambiguating a bare canonical reference (which falls back to a {@code SQLView} when no {@code
 * ViewDefinition} matches).
 *
 * @author John Grimes
 */
// Bean name is set explicitly to avoid colliding with Spring MVC's DispatcherServlet, which
// auto-discovers any bean named "viewResolver" and casts it to
// org.springframework.web.servlet.ViewResolver.
@Component("sqlQueryViewResolver")
public class ViewResolver {

  private static final String VIEW_DEFINITION = "ViewDefinition";

  @Nonnull private final ReadExecutor readExecutor;

  @Nonnull private final ServerConfiguration serverConfiguration;

  @Nonnull private final FhirContext fhirContext;

  @Nonnull private final Gson gson;

  /**
   * Constructs a new ViewResolver.
   *
   * @param readExecutor read executor for stored ViewDefinition resources
   * @param serverConfiguration server configuration (used for the auth toggle)
   * @param fhirContext FHIR context used to serialise the resolved ViewDefinition for parsing
   */
  @Autowired
  public ViewResolver(
      @Nonnull final ReadExecutor readExecutor,
      @Nonnull final ServerConfiguration serverConfiguration,
      @Nonnull final FhirContext fhirContext) {
    this.readExecutor = readExecutor;
    this.serverConfiguration = serverConfiguration;
    this.fhirContext = fhirContext;
    this.gson = ViewDefinitionGson.create();
  }

  /**
   * Resolves a {@code ViewDefinition} reference into a {@link ResolvedViewDefinition}, throwing
   * when no matching view is supplied or stored. Used for explicit {@code ViewDefinition/[id]}
   * references, where the contract forbids falling back to another resource type.
   *
   * @param reference the dependency reference to resolve
   * @param suppliedViews request-supplied views keyed by the ViewDefinition id they satisfy
   * @return the resolved leaf node
   * @throws InvalidRequestException if the reference cannot be resolved or parsed
   */
  @Nonnull
  public ResolvedViewDefinition resolveViewDefinition(
      @Nonnull final ViewArtifactReference reference,
      @Nonnull final Map<String, FhirView> suppliedViews) {
    return tryResolveViewDefinition(reference, suppliedViews)
        .orElseThrow(
            () ->
                new InvalidRequestException(
                    "Failed to resolve ViewDefinition for label '"
                        + reference.getLabel()
                        + "' with reference '"
                        + reference.getCanonicalUrl()
                        + "'"));
  }

  /**
   * Attempts to resolve a {@code ViewDefinition} reference into a {@link ResolvedViewDefinition},
   * returning empty when no request-supplied view satisfies it and no stored ViewDefinition with
   * the reference's id exists. Used as the first step of disambiguating a bare canonical reference,
   * which falls back to a {@code SQLView} when this returns empty.
   *
   * <p>A request-supplied view is preferred over storage and carries its own authorisation as part
   * of the request payload. A stored view is subject to the per-projected-resource-type READ check
   * (and, when authorisation is enabled, the {@code ViewDefinition} metadata READ check).
   *
   * @param reference the dependency reference to resolve
   * @param suppliedViews request-supplied views keyed by the ViewDefinition id they satisfy
   * @return the resolved leaf node, or empty if no ViewDefinition matches
   * @throws InvalidRequestException if a stored ViewDefinition exists but cannot be parsed
   */
  @Nonnull
  public Optional<ResolvedViewDefinition> tryResolveViewDefinition(
      @Nonnull final ViewArtifactReference reference,
      @Nonnull final Map<String, FhirView> suppliedViews) {
    final String viewDefinitionId = extractViewDefinitionId(reference.getCanonicalUrl());
    final String canonicalKey = VIEW_DEFINITION + "/" + viewDefinitionId;

    // Prefer a request-supplied view that satisfies this reference; it carries its own
    // authorisation as the request payload and is not read from storage.
    final FhirView suppliedView = suppliedViews.get(viewDefinitionId);
    if (suppliedView != null) {
      return Optional.of(new ResolvedViewDefinition(canonicalKey, suppliedView));
    }

    final Optional<IBaseResource> stored = readViewDefinition(viewDefinitionId);
    if (stored.isEmpty()) {
      return Optional.empty();
    }

    // The ViewDefinition was read from storage: enforce the metadata READ check, then parse it and
    // enforce the per-projected-resource READ check.
    checkMetadataReadAuthority(VIEW_DEFINITION);
    final FhirView view = parseViewDefinition(stored.get());
    checkProjectedResourceReadAuthority(view);
    return Optional.of(new ResolvedViewDefinition(canonicalKey, view));
  }

  /**
   * Reads a stored ViewDefinition by its logical id, mapping a missing resource to an empty result
   * so the caller can fall back to another resolution strategy.
   */
  @Nonnull
  private Optional<IBaseResource> readViewDefinition(@Nonnull final String id) {
    try {
      return Optional.of(readExecutor.read(VIEW_DEFINITION, id));
    } catch (final ResourceNotFoundError e) {
      return Optional.empty();
    } catch (final IllegalArgumentException e) {
      if (e.getMessage() != null && e.getMessage().contains("No data found for resource type")) {
        return Optional.empty();
      }
      throw e;
    }
  }

  /**
   * Extracts a ViewDefinition id from a canonical URL or relative reference. Supports relative
   * references like {@code ViewDefinition/my-id} and bare ids.
   */
  @Nonnull
  private String extractViewDefinitionId(@Nonnull final String canonicalUrl) {
    if (canonicalUrl.contains("/")) {
      final String[] parts = canonicalUrl.split("/");
      return parts[parts.length - 1];
    }
    return canonicalUrl;
  }

  /** Parses a ViewDefinition resource into a FhirView via JSON round-tripping. */
  @Nonnull
  private FhirView parseViewDefinition(@Nonnull final IBaseResource viewResource) {
    try {
      final String viewJson = fhirContext.newJsonParser().encodeResourceToString(viewResource);
      return gson.fromJson(viewJson, FhirView.class);
    } catch (final JsonSyntaxException e) {
      throw new InvalidRequestException("Invalid ViewDefinition: " + e.getMessage());
    }
  }

  /**
   * Enforces the metadata READ check for a resource resolved from storage, when authorisation is
   * enabled. Reading a {@code ViewDefinition} out of the server requires READ authority on the
   * {@code ViewDefinition} type itself, independent of the data the view projects.
   */
  private void checkMetadataReadAuthority(@Nonnull final String resourceType) {
    if (serverConfiguration.getAuth().isEnabled()) {
      SecurityAspect.checkHasAuthority(
          PathlingAuthority.resourceAccess(AccessType.READ, resourceType));
    }
  }

  /**
   * Enforces the per-projected-resource-type READ check for a parsed view, when authorisation is
   * enabled.
   */
  private void checkProjectedResourceReadAuthority(@Nonnull final FhirView view) {
    if (serverConfiguration.getAuth().isEnabled()) {
      SecurityAspect.checkHasAuthority(
          PathlingAuthority.resourceAccess(AccessType.READ, view.getResource()));
    }
  }
}
