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
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.encoders.ViewDefinitionResource;
import au.csiro.pathling.io.source.DataSource;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.functions;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Resolves a {@link ViewArtifactReference} that targets a {@code ViewDefinition} into a {@link
 * ResolvedViewDefinition} leaf node, matching the reference's canonical URL against the stored
 * {@code ViewDefinition.url} (the logical id plays no part). A request-supplied view is preferred
 * over storage, matched by its URL. Has no Spark execution dependency; the {@link FhirView} is the
 * parsed view configuration, not yet a {@code Dataset}.
 *
 * <p>Resolution is split into the supplied-view step and the stored-lookup step so the caller can
 * detect an ambiguous reference (a URL matching both a {@code ViewDefinition} and a {@code SQLView
 * Library}) before committing to either.
 *
 * @author John Grimes
 */
// Bean name is set explicitly to avoid colliding with Spring MVC's DispatcherServlet, which
// auto-discovers any bean named "viewResolver" and casts it to
// org.springframework.web.servlet.ViewResolver.
@Component("sqlQueryViewResolver")
public class ViewResolver {

  private static final String VIEW_DEFINITION = "ViewDefinition";

  private static final String ACTIVE_STATUS = "active";

  @Nonnull private final DataSource dataSource;

  @Nonnull private final FhirEncoders fhirEncoders;

  @Nonnull private final ServerConfiguration serverConfiguration;

  @Nonnull private final FhirContext fhirContext;

  @Nonnull private final Gson gson;

  /**
   * Constructs a new ViewResolver.
   *
   * @param dataSource the data source used to match stored ViewDefinitions by url
   * @param fhirEncoders FHIR encoders used to decode the matched ViewDefinition rows
   * @param serverConfiguration server configuration (used for the auth toggle)
   * @param fhirContext FHIR context used to serialise the resolved ViewDefinition for parsing
   */
  @Autowired
  public ViewResolver(
      @Nonnull final DataSource dataSource,
      @Nonnull final FhirEncoders fhirEncoders,
      @Nonnull final ServerConfiguration serverConfiguration,
      @Nonnull final FhirContext fhirContext) {
    this.dataSource = dataSource;
    this.fhirEncoders = fhirEncoders;
    this.serverConfiguration = serverConfiguration;
    this.fhirContext = fhirContext;
    this.gson = ViewDefinitionGson.create();
  }

  /**
   * Resolves a reference against a request-supplied view, preferring it over storage. A supplied
   * view satisfies a reference when its URL matches the reference's url. It carries its own
   * authorisation as the request payload, so no metadata or projected-resource READ check applies.
   *
   * @param reference the dependency reference to resolve
   * @param suppliedViews request-supplied views keyed by the canonical URL they satisfy
   * @return the resolved leaf node, or empty if no supplied view matches the reference's url
   */
  @Nonnull
  public Optional<ResolvedViewDefinition> resolveSuppliedView(
      @Nonnull final ViewArtifactReference reference,
      @Nonnull final Map<String, FhirView> suppliedViews) {
    final CanonicalReference canonical = CanonicalReference.parse(reference.getCanonicalUrl());
    final FhirView supplied = suppliedViews.get(canonical.getUrl());
    if (supplied == null) {
      return Optional.empty();
    }
    // A supplied view carries no version; its identity is its URL.
    return Optional.of(new ResolvedViewDefinition(canonical.getUrl(), supplied));
  }

  /**
   * Resolves a reference against a stored {@code ViewDefinition}, matching the reference's url (and
   * version, when supplied) against {@code ViewDefinition.url}. Returns empty when no stored
   * ViewDefinition matches, so the caller can fall back to a {@code SQLView}.
   *
   * <p>When a ViewDefinition matches, the {@code ViewDefinition} metadata READ check and the
   * per-projected-resource-type READ check are enforced (when authorisation is enabled).
   *
   * @param reference the dependency reference to resolve
   * @return the resolved leaf node, or empty if no stored ViewDefinition matches
   * @throws InvalidRequestException if a matching ViewDefinition cannot be parsed
   */
  @Nonnull
  public Optional<ResolvedViewDefinition> resolveStoredViewDefinition(
      @Nonnull final ViewArtifactReference reference) {
    final CanonicalReference canonical = CanonicalReference.parse(reference.getCanonicalUrl());
    final List<IBaseResource> matches = matchByUrl(canonical);
    if (matches.isEmpty()) {
      return Optional.empty();
    }

    final ViewDefinitionResource chosen =
        (ViewDefinitionResource)
            canonical.select(matches, ViewResolver::isActive, ViewResolver::versionOf);

    // The ViewDefinition was read from storage: enforce the metadata READ check, then parse it and
    // enforce the per-projected-resource READ check.
    checkMetadataReadAuthority();
    final FhirView view = parseViewDefinition(chosen);
    checkProjectedResourceReadAuthority(view);

    final String canonicalKey = CanonicalReference.key(chosen.getUrl(), chosen.getVersion());
    return Optional.of(new ResolvedViewDefinition(canonicalKey, view));
  }

  /**
   * Matches stored ViewDefinitions by the reference's url (and version, when supplied), returning
   * the decoded resources. When the server holds no ViewDefinition data at all, the reference
   * simply does not match, so an empty list is returned rather than surfacing the data source's
   * missing-type error.
   */
  @Nonnull
  private List<IBaseResource> matchByUrl(@Nonnull final CanonicalReference canonical) {
    final Dataset<Row> viewDefinitions;
    try {
      viewDefinitions = dataSource.read(VIEW_DEFINITION);
    } catch (final IllegalArgumentException e) {
      if (isMissingResourceType(e)) {
        return List.of();
      }
      throw e;
    }
    Dataset<Row> filtered =
        viewDefinitions.filter(viewDefinitions.col("url").equalTo(canonical.getUrl()));
    if (canonical.hasVersion()) {
      filtered = filtered.filter(functions.col("version").equalTo(canonical.getVersion()));
    }
    final ExpressionEncoder<IBaseResource> encoder = fhirEncoders.of(VIEW_DEFINITION);
    return filtered.as(encoder).collectAsList();
  }

  /**
   * Indicates whether an {@link IllegalArgumentException} signals that the data source holds no
   * data for the requested resource type (as opposed to a genuine error).
   */
  private static boolean isMissingResourceType(@Nonnull final IllegalArgumentException e) {
    return e.getMessage() != null && e.getMessage().contains("No data found for resource type");
  }

  /** Returns whether a decoded ViewDefinition carries {@code active} status. */
  private static boolean isActive(@Nonnull final IBaseResource resource) {
    final ViewDefinitionResource view = (ViewDefinitionResource) resource;
    return view.getStatusElement() != null
        && ACTIVE_STATUS.equals(view.getStatusElement().getValueAsString());
  }

  /** Returns the version of a decoded ViewDefinition, or null when it has none. */
  private static String versionOf(@Nonnull final IBaseResource resource) {
    return ((ViewDefinitionResource) resource).getVersion();
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
   * Enforces the metadata READ check for a ViewDefinition resolved from storage, when authorisation
   * is enabled. Reading a {@code ViewDefinition} out of the server requires READ authority on the
   * {@code ViewDefinition} type itself, independent of the data the view projects.
   */
  private void checkMetadataReadAuthority() {
    if (serverConfiguration.getAuth().isEnabled()) {
      SecurityAspect.checkHasAuthority(
          PathlingAuthority.resourceAccess(AccessType.READ, VIEW_DEFINITION));
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
