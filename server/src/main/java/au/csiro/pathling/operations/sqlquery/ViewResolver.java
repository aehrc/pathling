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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Resolves the {@link ViewArtifactReference}s declared by a SQLQuery Library into parsed {@link
 * FhirView}s, performing the per-resource-type authorisation check along the way. Has no Spark
 * dependency; the FhirView is the parsed view configuration, not yet a {@code Dataset}.
 */
@Component
public class ViewResolver {

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
   * Resolves each view reference to a parsed {@link FhirView}, preserving label order.
   *
   * @param references the references declared by the SQLQuery Library, keyed by table label
   * @return a map from label to resolved FhirView
   * @throws InvalidRequestException if a reference cannot be resolved or parsed
   */
  @Nonnull
  public Map<String, FhirView> resolve(@Nonnull final List<ViewArtifactReference> references) {
    final Map<String, FhirView> resolved = new LinkedHashMap<>();

    for (final ViewArtifactReference ref : references) {
      final String viewDefinitionId = extractViewDefinitionId(ref.getCanonicalUrl());
      final IBaseResource viewResource;
      try {
        viewResource = readExecutor.read("ViewDefinition", viewDefinitionId);
      } catch (final Exception e) {
        throw new InvalidRequestException(
            "Failed to resolve ViewDefinition for label '"
                + ref.getLabel()
                + "' with reference '"
                + ref.getCanonicalUrl()
                + "': "
                + e.getMessage());
      }

      final FhirView view = parseViewDefinition(viewResource);

      if (serverConfiguration.getAuth().isEnabled()) {
        SecurityAspect.checkHasAuthority(
            PathlingAuthority.resourceAccess(AccessType.READ, view.getResource()));
      }

      resolved.put(ref.getLabel(), view);
    }

    return resolved;
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
}
