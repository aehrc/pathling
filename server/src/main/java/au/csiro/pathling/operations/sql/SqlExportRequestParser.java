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

import static au.csiro.pathling.operations.sql.SubjectResolver.SUBJECT_EXPRESSION;

import au.csiro.pathling.operations.sqlquery.PreparedSqlQuery;
import au.csiro.pathling.operations.sqlquery.ResolvedDependency;
import au.csiro.pathling.views.FhirView;
import ca.uhn.fhir.rest.api.RequestTypeEnum;
import ca.uhn.fhir.rest.server.exceptions.BaseServerResponseException;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.servlet.http.HttpServletResponse;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.InstantType;
import org.hl7.fhir.r4.model.OperationOutcome.IssueType;
import org.hl7.fhir.r4.model.OperationOutcome.OperationOutcomeIssueComponent;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.hl7.fhir.r4.model.Reference;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Parses the raw {@code $sql-export} kick-off body into a validated {@link SqlExportRequest}.
 *
 * <p>Everything that can be decided without touching the data is decided here, at kick-off: the
 * operation promises that a job which starts will not be rejected later at its status URL. Each
 * subject is resolved, named, and - for a SQL subject - parsed, parameter bound, dependency
 * resolved and statically validated, so background execution can fail only on the data itself.
 *
 * <p>Failures across subjects and filters are accumulated rather than thrown one at a time, so that
 * a body with several problems is answered with one {@code OperationOutcome} naming them all.
 * Dependency resolution is memoised across the job, so a canonical URL shared by several subjects
 * is resolved once.
 *
 * @author John Grimes
 */
@Component
public class SqlExportRequestParser {

  private static final String SUBJECT_PART = "subject";

  private static final String PARAMETERS_PART = "parameters";

  private static final String LIMIT_PARAMETER = "_limit";

  private static final String SOURCE_PARAMETER = "source";

  @Nonnull private final SubjectResolver subjectResolver;

  @Nonnull private final SqlFilterResolver filterResolver;

  @Nonnull private final ContextArtefactParser contextParser;

  @Nonnull private final FhirViewValidator viewValidator;

  @Nonnull private final au.csiro.pathling.operations.sqlquery.SqlQueryPipeline pipeline;

  /**
   * Constructs a new SqlExportRequestParser.
   *
   * @param subjectResolver resolves each subject repetition and detects its kind
   * @param filterResolver resolves the patient, group and {@code _since} filters
   * @param contextParser parses the job-wide inline supporting artefacts
   * @param viewValidator parses and validates a ViewDefinition subject
   * @param pipeline prepares and statically validates a SQL subject
   */
  @Autowired
  public SqlExportRequestParser(
      @Nonnull final SubjectResolver subjectResolver,
      @Nonnull final SqlFilterResolver filterResolver,
      @Nonnull final ContextArtefactParser contextParser,
      @Nonnull final FhirViewValidator viewValidator,
      @Nonnull final au.csiro.pathling.operations.sqlquery.SqlQueryPipeline pipeline) {
    this.subjectResolver = subjectResolver;
    this.filterResolver = filterResolver;
    this.contextParser = contextParser;
    this.viewValidator = viewValidator;
    this.pipeline = pipeline;
  }

  /**
   * Parses and validates the kick-off request.
   *
   * @param requestDetails the servlet request details, carrying the body, verb and headers
   * @return the validated request
   * @throws ca.uhn.fhir.rest.server.exceptions.BaseServerResponseException carrying a populated
   *     {@code OperationOutcome} for any validation failure
   */
  @Nonnull
  public SqlExportRequest parse(@Nonnull final ServletRequestDetails requestDetails) {
    rejectGet(requestDetails);
    requireRespondAsync(requestDetails);

    final Parameters parameters = parametersOf(requestDetails);

    rejectSource(stringParam(parameters, SOURCE_PARAMETER));
    rejectLimit(parameters);

    final SqlExportFormat format = SqlExportFormat.fromString(stringParam(parameters, "_format"));
    final boolean includeHeader = booleanParam(parameters, "header");
    final String clientTrackingId = stringParam(parameters, "clientTrackingId");

    final SuppliedArtefacts supplied = contextParser.parse(contextEntries(parameters));

    final ResolvedFilters filters =
        filterResolver.resolve(
            references(parameters, "patient"),
            references(parameters, "group"),
            instantParam(parameters, "_since"));

    final List<SubjectInput> subjects = parseSubjects(parameters, supplied, filters);

    // An entry reachable only through another supplied entry is matched during the traversal above,
    // so unmatched entries can only be detected once every subject has been prepared.
    supplied.checkAllMatched();

    return new SqlExportRequest(
        requestDetails.getCompleteUrl(),
        requestDetails.getFhirServerBase(),
        subjects,
        clientTrackingId,
        format,
        includeHeader,
        filters.patientIds(),
        filters.since());
  }

  /**
   * Parses every {@code subject} repetition, accumulating each repetition's failure so that all of
   * them, together with any filter failures, are reported in a single outcome.
   */
  @Nonnull
  private List<SubjectInput> parseSubjects(
      @Nonnull final Parameters parameters,
      @Nonnull final SuppliedArtefacts supplied,
      @Nonnull final ResolvedFilters filters) {

    final List<ParametersParameterComponent> repetitions = partsNamed(parameters, SUBJECT_PART);
    if (repetitions.isEmpty()) {
      throw SqlOperationError.badRequest(
          IssueType.REQUIRED,
          SUBJECT_EXPRESSION,
          "At least one 'subject' must be supplied; an export with no subject has nothing to"
              + " produce.");
    }

    final List<SubjectInput> subjects = new ArrayList<>();
    final List<OperationOutcomeIssueComponent> issues = new ArrayList<>();
    final Set<String> usedNames = new LinkedHashSet<>();
    // Shared across the job so a canonical URL reached from several subjects resolves once.
    final Map<String, ResolvedDependency> nodesByKey = new LinkedHashMap<>();
    int status = HttpServletResponse.SC_BAD_REQUEST;

    for (int i = 0; i < repetitions.size(); i++) {
      try {
        subjects.add(parseSubject(repetitions.get(i), i, supplied, nodesByKey, usedNames));
      } catch (final BaseServerResponseException e) {
        if (issues.isEmpty()) {
          // The first subject failure decides the status: a later, less specific failure must not
          // downgrade a 404 or 422 to a 400.
          status = e.getStatusCode();
        }
        issues.addAll(SqlOperationOutcomes.issuesOf(e, SUBJECT_EXPRESSION));
      }
    }

    issues.addAll(filters.issues());
    if (!issues.isEmpty()) {
      throw SqlOperationError.of(status, issues);
    }
    return subjects;
  }

  /** Parses a single {@code subject} repetition into its prepared form. */
  @Nonnull
  private SubjectInput parseSubject(
      @Nonnull final ParametersParameterComponent repetition,
      final int index,
      @Nonnull final SuppliedArtefacts supplied,
      @Nonnull final Map<String, ResolvedDependency> nodesByKey,
      @Nonnull final Set<String> usedNames) {

    final String suppliedName = partString(repetition, "name");
    final Parameters bindings =
        partResource(repetition, PARAMETERS_PART) instanceof final Parameters params
            ? params
            : null;

    final ResolvedSubject subject =
        subjectResolver.resolve(
            partString(repetition, "subjectCanonical"),
            partReference(repetition, "subjectReference"),
            partResource(repetition, "subjectResource"),
            suppliedName);

    if (subject.getKind() == SubjectKind.VIEW_DEFINITION && bindings != null) {
      throw SqlOperationError.badRequest(
          IssueType.INVALID,
          PARAMETERS_PART,
          "The 'parameters' part applies only to a SQLQuery or SQLView subject; a ViewDefinition"
              + " declares no parameters.");
    }

    final String name = subject.getEffectiveName(index);
    if (!usedNames.add(name)) {
      throw SqlOperationError.badRequest(
          IssueType.INVALID,
          SUBJECT_EXPRESSION,
          "Two subjects resolve to the output name '%s'; each subject's name must be unique across"
                  .formatted(name)
              + " the export.");
    }

    if (subject.getKind() == SubjectKind.VIEW_DEFINITION) {
      final FhirView view = viewValidator.parse(subject.getResource(), SUBJECT_EXPRESSION);
      viewValidator.checkProjectedResourceReadAuthority(view);
      viewValidator.validateSemantically(view, SUBJECT_EXPRESSION);
      return SubjectInput.ofView(name, view);
    }

    return SubjectInput.ofSql(
        subject.getKind(), name, prepare(subject, bindings, supplied, nodesByKey));
  }

  /**
   * Prepares a SQL subject, relabelling a binding failure onto the {@code parameters} part. A 400
   * raised while preparing a subject that supplied bindings is, in practice, always about those
   * bindings: the structural failures of the artefact itself surface as 404 or 422.
   */
  @Nonnull
  private PreparedSqlQuery prepare(
      @Nonnull final ResolvedSubject subject,
      @Nullable final Parameters bindings,
      @Nonnull final SuppliedArtefacts supplied,
      @Nonnull final Map<String, ResolvedDependency> nodesByKey) {
    try {
      final PreparedSqlQuery prepared =
          pipeline.prepare(
              subject.asLibrary(), null, null, null, null, bindings, supplied, nodesByKey);
      pipeline.validateStatically(prepared);
      return prepared;
    } catch (final InvalidRequestException e) {
      if (bindings == null || e.getOperationOutcome() != null) {
        throw e;
      }
      throw SqlOperationError.badRequest(IssueType.INVALID, PARAMETERS_PART, e.getMessage());
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Request-level rejections.
  // ---------------------------------------------------------------------------------------------

  /** Rejects a GET, which cannot carry the subjects and context an export needs. */
  private static void rejectGet(@Nonnull final ServletRequestDetails requestDetails) {
    if (requestDetails.getRequestType() == RequestTypeEnum.GET) {
      throw SqlOperationError.badRequest(
          IssueType.REQUIRED,
          null,
          "The $sql-export operation must be invoked with POST and a Parameters body; its subjects"
              + " and context cannot be expressed in a query string.");
    }
  }

  /** Rejects a request that has not asked for the asynchronous pattern. */
  private static void requireRespondAsync(@Nonnull final ServletRequestDetails requestDetails) {
    final String prefer = requestDetails.getHeader("Prefer");
    if (prefer == null || !prefer.toLowerCase().contains("respond-async")) {
      throw SqlOperationError.badRequest(
          IssueType.REQUIRED,
          null,
          "The $sql-export operation is asynchronous and requires the 'Prefer: respond-async'"
              + " header.");
    }
  }

  /** Rejects the unsupported external data source parameter. */
  private static void rejectSource(@Nullable final String source) {
    if (source != null && !source.isBlank()) {
      throw SqlOperationError.badRequest(
          IssueType.NOTSUPPORTED,
          SOURCE_PARAMETER,
          "The 'source' parameter (external data source) is not supported by this server.");
    }
  }

  /** Rejects {@code _limit}, which an export does not offer. */
  private static void rejectLimit(@Nonnull final Parameters parameters) {
    if (!partsNamed(parameters, LIMIT_PARAMETER).isEmpty()) {
      throw SqlOperationError.badRequest(
          IssueType.INVALID,
          LIMIT_PARAMETER,
          "The '_limit' parameter is not offered by $sql-export; an export writes the whole result"
              + " set.");
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Named-part lookup.
  // ---------------------------------------------------------------------------------------------

  @Nonnull
  private static Parameters parametersOf(@Nonnull final ServletRequestDetails requestDetails) {
    return requestDetails.getResource() instanceof final Parameters parameters
        ? parameters
        : new Parameters();
  }

  @Nonnull
  private static List<ParametersParameterComponent> partsNamed(
      @Nonnull final Parameters parameters, @Nonnull final String name) {
    return parameters.getParameter().stream().filter(p -> name.equals(p.getName())).toList();
  }

  @Nullable
  private static String stringParam(
      @Nonnull final Parameters parameters, @Nonnull final String name) {
    return partsNamed(parameters, name).stream()
        .filter(p -> p.getValue() != null)
        .map(p -> p.getValue().primitiveValue())
        .findFirst()
        .orElse(null);
  }

  private static boolean booleanParam(
      @Nonnull final Parameters parameters, @Nonnull final String name) {
    final String value = stringParam(parameters, name);
    return value == null || Boolean.parseBoolean(value);
  }

  @Nullable
  private static InstantType instantParam(
      @Nonnull final Parameters parameters, @Nonnull final String name) {
    final String value = stringParam(parameters, name);
    return value == null ? null : new InstantType(value);
  }

  @Nonnull
  private static List<Reference> references(
      @Nonnull final Parameters parameters, @Nonnull final String name) {
    return partsNamed(parameters, name).stream()
        .map(ParametersParameterComponent::getValue)
        .filter(Reference.class::isInstance)
        .map(Reference.class::cast)
        .toList();
  }

  @Nonnull
  private static List<IBaseResource> contextEntries(@Nonnull final Parameters parameters) {
    return partsNamed(parameters, SuppliedArtefacts.CONTEXT_EXPRESSION).stream()
        .map(ParametersParameterComponent::getResource)
        .filter(java.util.Objects::nonNull)
        .map(IBaseResource.class::cast)
        .toList();
  }

  @Nullable
  private static String partString(
      @Nonnull final ParametersParameterComponent repetition, @Nonnull final String name) {
    for (final ParametersParameterComponent part : repetition.getPart()) {
      if (name.equals(part.getName()) && part.getValue() != null) {
        return part.getValue().primitiveValue();
      }
    }
    return null;
  }

  @Nullable
  private static Reference partReference(
      @Nonnull final ParametersParameterComponent repetition, @Nonnull final String name) {
    for (final ParametersParameterComponent part : repetition.getPart()) {
      if (name.equals(part.getName()) && part.getValue() instanceof final Reference reference) {
        return reference;
      }
    }
    return null;
  }

  @Nullable
  private static IBaseResource partResource(
      @Nonnull final ParametersParameterComponent repetition, @Nonnull final String name) {
    for (final ParametersParameterComponent part : repetition.getPart()) {
      if (name.equals(part.getName()) && part.getResource() != null) {
        return part.getResource();
      }
    }
    return null;
  }
}
