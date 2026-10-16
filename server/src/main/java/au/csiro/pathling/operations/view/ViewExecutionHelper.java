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

package au.csiro.pathling.operations.view;

import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.errors.UnsupportedFhirPathFeatureError;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.security.PathlingAuthority;
import au.csiro.pathling.security.ResourceAccess.AccessType;
import au.csiro.pathling.security.SecurityAspect;
import au.csiro.pathling.views.FhirView;
import au.csiro.pathling.views.FhirViewExecutor;
import au.csiro.pathling.views.ViewDefinitionGson;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.exceptions.UnprocessableEntityException;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.validation.ConstraintViolationException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.IntegerType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Executes ViewDefinition subjects for {@code $sql-run}: parses the ViewDefinition, builds a data
 * source from inline resources, enforces the projected resource READ check and streams the result.
 *
 * @author John Grimes
 */
@Slf4j
@Component
public class ViewExecutionHelper {

  @Nonnull private final SparkSession sparkSession;

  @Nonnull private final FhirContext fhirContext;

  @Nonnull private final FhirEncoders fhirEncoders;

  @Nonnull private final ServerConfiguration serverConfiguration;

  @Nonnull private final Gson gson;

  @Nonnull private final ResultStreamingHelper streamingHelper;

  /**
   * Constructs a new ViewExecutionHelper.
   *
   * @param sparkSession the Spark session
   * @param fhirContext the FHIR context
   * @param fhirEncoders the FHIR encoders
   * @param serverConfiguration the server configuration
   */
  @Autowired
  public ViewExecutionHelper(
      @Nonnull final SparkSession sparkSession,
      @Nonnull final FhirContext fhirContext,
      @Nonnull final FhirEncoders fhirEncoders,
      @Nonnull final ServerConfiguration serverConfiguration) {
    this.sparkSession = sparkSession;
    this.fhirContext = fhirContext;
    this.fhirEncoders = fhirEncoders;
    this.serverConfiguration = serverConfiguration;
    this.gson = ViewDefinitionGson.create();
    this.streamingHelper = new ResultStreamingHelper(gson);
  }

  /**
   * Executes the view query and streams results to the output stream, which must not have been
   * written to, so that a failure in evaluating the first row can still be reported with an error
   * status.
   */
  private void executeAndStreamResults(
      @Nonnull final FhirView view,
      @Nonnull final DataSource dataSource,
      @Nullable final IntegerType limit,
      @Nonnull final ViewOutputFormat outputFormat,
      final boolean includeHeader,
      @Nonnull final OutputStream outputStream)
      throws IOException {

    final FhirViewExecutor executor =
        new FhirViewExecutor(fhirContext, dataSource, serverConfiguration.getQuery());
    Dataset<Row> result;
    try {
      result = executor.buildQuery(view);
    } catch (final ConstraintViolationException e) {
      // A well-formed request carrying a semantically invalid ViewDefinition maps to 422,
      // distinct from the 400 used for malformed requests and invalid parameters.
      throw new UnprocessableEntityException("Invalid ViewDefinition: " + e.getMessage());
    } catch (final UnsupportedOperationException | UnsupportedFhirPathFeatureError e) {
      // Thrown when a FHIRPath expression is not supported, such as accessing a choice element
      // without specifying the type via ofType().
      throw new InvalidRequestException("Unsupported expression: " + e.getMessage());
    }

    // Apply limit if specified.
    if (limit != null && limit.getValue() != null) {
      result = result.limit(limit.getValue());
    }

    // Stream results.
    final StructType schema = result.schema();
    final Iterator<Row> iterator = result.toLocalIterator();

    switch (outputFormat) {
      case NDJSON -> streamingHelper.streamNdjson(outputStream, iterator, schema);
      case JSON -> streamingHelper.writeJson(outputStream, iterator, schema);
      default -> streamingHelper.streamCsv(outputStream, iterator, schema, includeHeader);
    }
  }

  /**
   * Executes a parsed view against an already-built data source and streams the result, leaving the
   * caller to decide the format, the filtering and the data supply.
   *
   * @param view the parsed view to execute
   * @param dataSource the data source to project, already filtered
   * @param outputFormat the output format to emit
   * @param includeHeader whether to include a header row in CSV output
   * @param limit the maximum number of rows to return, applied after evaluation
   * @param response the HTTP response to stream to
   * @throws UnprocessableEntityException (422) if the view is semantically invalid
   * @throws InvalidRequestException (400) if the view uses an unsupported expression, or the result
   *     cannot be streamed
   */
  @SuppressWarnings("java:S107")
  public void streamView(
      @Nonnull final FhirView view,
      @Nonnull final DataSource dataSource,
      @Nonnull final ViewOutputFormat outputFormat,
      final boolean includeHeader,
      @Nullable final IntegerType limit,
      @Nonnull final HttpServletResponse response) {

    checkProjectedResourceReadAuthority(view);

    response.setContentType(outputFormat.getContentType());
    response.setCharacterEncoding(StandardCharsets.UTF_8.name());
    response.setStatus(HttpServletResponse.SC_OK);

    try {
      final OutputStream outputStream = response.getOutputStream();
      executeAndStreamResults(view, dataSource, limit, outputFormat, includeHeader, outputStream);
      outputStream.flush();
    } catch (final IOException e) {
      log.error("Error streaming view results", e);
      throw new InvalidRequestException("Error streaming results: " + e.getMessage());
    }
  }

  /**
   * Builds a data source from inline FHIR resources supplied with the request, used in place of
   * server data.
   *
   * @param inlineResources the serialised resources, each a resource or a Bundle to unwrap
   * @return a data source over the supplied resources
   * @throws InvalidRequestException (400) if a supplied resource cannot be parsed
   */
  @Nonnull
  public DataSource inlineDataSource(@Nonnull final List<String> inlineResources) {
    return new ObjectDataSource(sparkSession, fhirEncoders, parseInlineResources(inlineResources));
  }

  /**
   * Enforces the per-projected-resource-type READ check for a parsed view, when authorisation is
   * enabled.
   *
   * @param view the parsed view whose subject resource type is checked
   */
  public void checkProjectedResourceReadAuthority(@Nonnull final FhirView view) {
    if (serverConfiguration.getAuth().isEnabled()) {
      SecurityAspect.checkHasAuthority(
          PathlingAuthority.resourceAccess(AccessType.READ, view.getResource()));
    }
  }

  /**
   * Parses a ViewDefinition resource into a {@link FhirView}.
   *
   * <p>The HAPI resource is serialised back to JSON and parsed with Gson, which avoids duplicating
   * the FhirView class hierarchy as HAPI resource components.
   *
   * @param viewResource the ViewDefinition resource to parse
   * @return the parsed view
   * @throws InvalidRequestException (400) if the resource is not a well-formed ViewDefinition
   */
  @Nonnull
  public FhirView parseViewDefinition(@Nonnull final IBaseResource viewResource) {
    try {
      // Serialise the HAPI resource back to JSON.
      final String viewJson = fhirContext.newJsonParser().encodeResourceToString(viewResource);
      // Parse the JSON into the FhirView class.
      return gson.fromJson(viewJson, FhirView.class);
    } catch (final JsonSyntaxException e) {
      throw new InvalidRequestException("Invalid ViewDefinition: " + e.getMessage());
    }
  }

  /**
   * Parses inline FHIR resources from JSON strings, unwrapping any {@code Bundle} value into its
   * entry resources. A {@code Bundle} contributes its {@code entry[*].resource} members (one level
   * of unwrapping); standalone resources are used directly. The data source is the union of all
   * standalone resources and all unwrapped Bundle entry resources; an empty Bundle contributes
   * nothing.
   */
  @Nonnull
  private List<IBaseResource> parseInlineResources(@Nonnull final List<String> inlineResources) {
    final IParser jsonParser = fhirContext.newJsonParser();
    final List<IBaseResource> resources = new ArrayList<>();
    for (final String resourceJson : inlineResources) {
      final IBaseResource parsed;
      try {
        parsed = jsonParser.parseResource(resourceJson);
      } catch (final Exception e) {
        throw new InvalidRequestException("Invalid inline resource: " + e.getMessage());
      }
      if (parsed instanceof final Bundle bundle) {
        for (final Bundle.BundleEntryComponent entry : bundle.getEntry()) {
          if (entry.hasResource()) {
            resources.add(entry.getResource());
          }
        }
      } else {
        resources.add(parsed);
      }
    }
    return resources;
  }
}
