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

package au.csiro.pathling.operations.sql;

import static au.csiro.pathling.operations.sql.SuppliedArtefacts.CONTEXT_EXPRESSION;

import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.encoders.ViewDefinitionResource;
import au.csiro.pathling.errors.UnsupportedFhirPathFeatureError;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.operations.sqlquery.SqlLibraryParser;
import au.csiro.pathling.views.FhirView;
import au.csiro.pathling.views.FhirViewExecutor;
import au.csiro.pathling.views.ViewDefinitionGson;
import ca.uhn.fhir.context.FhirContext;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.validation.ConstraintViolationException;
import java.util.ArrayList;
import java.util.List;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Library;
import org.hl7.fhir.r4.model.OperationOutcome.IssueType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Parses the repeating {@code context} parameter into {@link SuppliedArtefacts}, the inline
 * supporting artefacts a request offers for dependencies the server cannot resolve.
 *
 * <p>Only a {@code ViewDefinition} or a {@code SQLView} {@code Library} can back a dependency, and
 * an entry must carry a {@code url}, since it is matched by canonical URL and one without a URL can
 * never match anything. A supplied ViewDefinition is validated semantically here, so a malformed or
 * unsatisfiable view is reported at request time rather than part-way through execution.
 *
 * @author John Grimes
 */
@Component
public class ContextArtefactParser {

  @Nonnull private final FhirContext fhirContext;

  @Nonnull private final ServerConfiguration serverConfiguration;

  @Nonnull private final QueryableDataSource deltaLake;

  @Nonnull private final Gson gson;

  /**
   * Constructs a new ContextArtefactParser.
   *
   * @param fhirContext the FHIR context, used to serialise supplied ViewDefinitions for parsing
   * @param serverConfiguration the server configuration, consulted for the query configuration
   * @param deltaLake the data source against which supplied ViewDefinitions are validated
   */
  @Autowired
  public ContextArtefactParser(
      @Nonnull final FhirContext fhirContext,
      @Nonnull final ServerConfiguration serverConfiguration,
      @Nonnull final QueryableDataSource deltaLake) {
    this.fhirContext = fhirContext;
    this.serverConfiguration = serverConfiguration;
    this.deltaLake = deltaLake;
    this.gson = ViewDefinitionGson.create();
  }

  /**
   * Parses the supplied {@code context} entries.
   *
   * @param entries the resources supplied as {@code context}, in request order
   * @return the parsed collection, empty when nothing was supplied
   * @throws ca.uhn.fhir.rest.server.exceptions.InvalidRequestException (400) if an entry is of an
   *     inadmissible kind, carries no {@code url}, or shares a {@code url} with another entry
   * @throws ca.uhn.fhir.rest.server.exceptions.UnprocessableEntityException (422) if a supplied
   *     ViewDefinition is semantically invalid
   */
  @Nonnull
  public SuppliedArtefacts parse(@Nullable final List<IBaseResource> entries) {
    if (entries == null || entries.isEmpty()) {
      return SuppliedArtefacts.empty();
    }
    final List<SuppliedArtefact> parsed = new ArrayList<>();
    for (final IBaseResource entry : entries) {
      parsed.add(parseEntry(entry));
    }
    return SuppliedArtefacts.of(parsed);
  }

  /** Parses a single entry into its supplied-artefact form. */
  @Nonnull
  private SuppliedArtefact parseEntry(@Nonnull final IBaseResource entry) {
    if (entry instanceof final ViewDefinitionResource viewDefinition) {
      final String url = requireUrl(viewDefinition.getUrl(), "ViewDefinition");
      final FhirView view = parseView(viewDefinition);
      validateSemantically(view);
      return SuppliedArtefact.ofView(url, viewDefinition.getVersion(), view);
    }
    if (entry instanceof final Library library) {
      if (!isSqlView(library)) {
        throw SqlOperationError.badRequest(
            IssueType.INVALID,
            CONTEXT_EXPRESSION,
            "A 'context' Library must conform to the SQLView profile; only a ViewDefinition or a"
                + " SQLView can back a dependency.");
      }
      final String url = requireUrl(library.getUrl(), "SQLView");
      return SuppliedArtefact.ofSqlView(url, library.getVersion(), library);
    }
    throw SqlOperationError.badRequest(
        IssueType.INVALID,
        CONTEXT_EXPRESSION,
        "A 'context' entry must be a ViewDefinition or a SQLView Library, but a %s was supplied."
            .formatted(entry.fhirType()));
  }

  /** Rejects an entry with no canonical URL, which can never match a dependency reference. */
  @Nonnull
  private static String requireUrl(@Nullable final String url, @Nonnull final String kind) {
    if (url == null || url.isBlank()) {
      throw SqlOperationError.badRequest(
          IssueType.INVALID,
          CONTEXT_EXPRESSION,
          "A 'context' %s must carry a url, since entries are matched to dependencies by canonical"
                  .formatted(kind)
              + " URL.");
    }
    return url;
  }

  /** Indicates whether a Library carries the SQL on FHIR SQLView type coding. */
  private static boolean isSqlView(@Nonnull final Library library) {
    final CodeableConcept type = library.getType();
    if (type == null || type.isEmpty()) {
      return false;
    }
    for (final Coding coding : type.getCoding()) {
      if (SqlLibraryParser.LIBRARY_TYPE_SYSTEM.equals(coding.getSystem())
          && SqlLibraryParser.SQL_VIEW_TYPE_CODE.equals(coding.getCode())) {
        return true;
      }
    }
    return false;
  }

  /** Parses a ViewDefinition resource into a FhirView via JSON round-tripping. */
  @Nonnull
  private FhirView parseView(@Nonnull final IBaseResource viewResource) {
    try {
      return gson.fromJson(
          fhirContext.newJsonParser().encodeResourceToString(viewResource), FhirView.class);
    } catch (final JsonSyntaxException e) {
      throw SqlOperationError.badRequest(
          IssueType.INVALID,
          CONTEXT_EXPRESSION,
          "A 'context' ViewDefinition could not be parsed: " + e.getMessage());
    }
  }

  /**
   * Validates a supplied ViewDefinition by building its query plan, so a view that cannot be
   * executed is reported before any subject runs.
   */
  private void validateSemantically(@Nonnull final FhirView view) {
    try {
      new FhirViewExecutor(fhirContext, deltaLake, serverConfiguration.getQuery()).buildQuery(view);
    } catch (final ConstraintViolationException e) {
      throw SqlOperationError.unprocessable(
          CONTEXT_EXPRESSION, "A 'context' ViewDefinition is invalid: " + e.getMessage());
    } catch (final UnsupportedOperationException | UnsupportedFhirPathFeatureError e) {
      throw SqlOperationError.badRequest(
          IssueType.NOTSUPPORTED,
          CONTEXT_EXPRESSION,
          "A 'context' ViewDefinition uses an unsupported expression: " + e.getMessage());
    }
  }
}
