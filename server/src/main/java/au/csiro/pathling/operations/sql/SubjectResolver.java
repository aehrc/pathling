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

import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.encoders.ViewDefinitionResource;
import au.csiro.pathling.errors.ResourceNotFoundError;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.operations.sqlquery.CanonicalReference;
import au.csiro.pathling.operations.sqlquery.SqlLibraryParser;
import au.csiro.pathling.read.ReadExecutor;
import au.csiro.pathling.security.PathlingAuthority;
import au.csiro.pathling.security.ResourceAccess.AccessType;
import au.csiro.pathling.security.SecurityAspect;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.functions;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.PublicationStatus;
import org.hl7.fhir.r4.model.Library;
import org.hl7.fhir.r4.model.OperationOutcome.IssueType;
import org.hl7.fhir.r4.model.Reference;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Resolves the subject of a {@code $sql-run} or {@code $sql-export} invocation from its three
 * mutually exclusive naming forms - {@code subjectCanonical}, {@code subjectReference} and {@code
 * subjectResource} - into a {@link ResolvedSubject} carrying the artefact and the kind it conforms
 * to.
 *
 * <p>Both operations are subject-polymorphic by contract, so kind detection lives here and is the
 * single source of truth for the conditional parameter rules ({@code parameters}, {@code resource})
 * and the per-kind output format sets that depend on it.
 *
 * <p>Resolution admits both artefact families. A canonical URL is matched against {@code
 * ViewDefinition.url} and {@code Library.url}; a URL matching both is ambiguous and rejected rather
 * than silently preferring one. A literal reference must name its type ({@code ViewDefinition/[id]}
 * or {@code Library/[id]}), since without it the family to read from is undetermined. Artefacts
 * read from storage are subject to the resource-level READ check; an inline artefact carries its
 * own content as the request payload and is not.
 *
 * @author John Grimes
 */
@Component
public class SubjectResolver {

  /** The {@code expression} value naming the subject in error outcomes. */
  public static final String SUBJECT_EXPRESSION = "subject";

  private static final String VIEW_DEFINITION = "ViewDefinition";

  private static final String LIBRARY = "Library";

  private static final String ACTIVE_STATUS = "active";

  @Nonnull private final ReadExecutor readExecutor;

  @Nonnull private final DataSource dataSource;

  @Nonnull private final FhirEncoders fhirEncoders;

  @Nonnull private final ServerConfiguration serverConfiguration;

  /**
   * Constructs a new SubjectResolver.
   *
   * @param readExecutor reads stored artefacts by logical id, for literal references
   * @param dataSource the data source searched when matching a canonical URL
   * @param fhirEncoders FHIR encoders used to decode the matched rows
   * @param serverConfiguration the server configuration, consulted for the authorisation toggle
   */
  @Autowired
  public SubjectResolver(
      @Nonnull final ReadExecutor readExecutor,
      @Nonnull final DataSource dataSource,
      @Nonnull final FhirEncoders fhirEncoders,
      @Nonnull final ServerConfiguration serverConfiguration) {
    this.readExecutor = readExecutor;
    this.dataSource = dataSource;
    this.fhirEncoders = fhirEncoders;
    this.serverConfiguration = serverConfiguration;
  }

  /**
   * Resolves a subject from its naming forms, of which exactly one must be supplied.
   *
   * @param subjectCanonical the {@code subjectCanonical} value, honouring a {@code |version} pin
   * @param subjectReference the {@code subjectReference} value, a relative literal reference
   * @param subjectResource the {@code subjectResource} value, an inline artefact
   * @param suppliedName the {@code subject.name} supplied alongside, or null when none was
   * @return the resolved subject
   * @throws ca.uhn.fhir.rest.server.exceptions.InvalidRequestException (400) if no naming form, or
   *     more than one, is supplied, or a reference cannot name an artefact family
   * @throws ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException (404) if a canonical or
   *     reference resolves to nothing
   * @throws ca.uhn.fhir.rest.server.exceptions.UnprocessableEntityException (422) if the resolved
   *     artefact conforms to none of the three admitted profiles
   */
  @Nonnull
  public ResolvedSubject resolve(
      @Nullable final String subjectCanonical,
      @Nullable final Reference subjectReference,
      @Nullable final IBaseResource subjectResource,
      @Nullable final String suppliedName) {

    final boolean hasCanonical = subjectCanonical != null && !subjectCanonical.isBlank();
    final boolean hasReference = subjectReference != null && !subjectReference.isEmpty();
    final boolean hasResource = subjectResource != null;

    final int supplied = (hasCanonical ? 1 : 0) + (hasReference ? 1 : 0) + (hasResource ? 1 : 0);
    if (supplied == 0) {
      throw SqlOperationError.badRequest(
          IssueType.REQUIRED,
          SUBJECT_EXPRESSION,
          "A subject must be named by exactly one of 'subjectCanonical', 'subjectReference' or"
              + " 'subjectResource', but none was supplied.");
    }
    if (supplied > 1) {
      throw SqlOperationError.badRequest(
          IssueType.INVALID,
          SUBJECT_EXPRESSION,
          "A subject must be named by exactly one of 'subjectCanonical', 'subjectReference' or"
              + " 'subjectResource', but more than one was supplied.");
    }

    // The requireNonNull calls restate what the hasResource, hasCanonical and hasReference guards
    // already establish, in a form that static analysis can verify.
    final IBaseResource artefact;
    if (hasResource) {
      artefact = subjectResource;
    } else if (hasCanonical) {
      artefact = resolveCanonical(Objects.requireNonNull(subjectCanonical));
    } else {
      artefact = resolveReference(Objects.requireNonNull(subjectReference));
    }

    return new ResolvedSubject(detectKind(artefact), artefact, suppliedName);
  }

  /**
   * Detects which of the three admitted profiles an artefact conforms to.
   *
   * @param artefact the resolved artefact
   * @return the detected kind
   * @throws ca.uhn.fhir.rest.server.exceptions.UnprocessableEntityException (422) if the artefact
   *     conforms to none of them
   */
  @Nonnull
  private static SubjectKind detectKind(@Nonnull final IBaseResource artefact) {
    if (artefact instanceof ViewDefinitionResource) {
      return SubjectKind.VIEW_DEFINITION;
    }
    if (artefact instanceof final Library library) {
      final SubjectKind kind = sqlLibraryKind(library);
      if (kind != null) {
        return kind;
      }
      throw SqlOperationError.unprocessable(
          SUBJECT_EXPRESSION,
          "The subject Library conforms to neither the SQLQuery nor the SQLView profile: its"
              + " Library.type must carry a coding from "
              + SqlLibraryParser.LIBRARY_TYPE_SYSTEM
              + ".");
    }
    throw SqlOperationError.unprocessable(
        SUBJECT_EXPRESSION,
        "The subject resolved to a %s, which is none of ViewDefinition, SQLQuery or SQLView."
            .formatted(artefact.fhirType()));
  }

  /**
   * Maps a {@code Library.type} coding from the SQL on FHIR library-types code system to the
   * matching subject kind, or null when the Library carries no recognised coding.
   */
  @Nullable
  private static SubjectKind sqlLibraryKind(@Nonnull final Library library) {
    final CodeableConcept type = library.getType();
    if (type == null || type.isEmpty()) {
      return null;
    }
    for (final Coding coding : type.getCoding()) {
      if (!SqlLibraryParser.LIBRARY_TYPE_SYSTEM.equals(coding.getSystem())) {
        continue;
      }
      if (SqlLibraryParser.SQL_QUERY_TYPE_CODE.equals(coding.getCode())) {
        return SubjectKind.SQL_QUERY;
      }
      if (SqlLibraryParser.SQL_VIEW_TYPE_CODE.equals(coding.getCode())) {
        return SubjectKind.SQL_VIEW;
      }
    }
    return null;
  }

  /**
   * Resolves a canonical URL against stored ViewDefinitions and Libraries, honouring a {@code
   * |version} pin. Both families are searched so that the caller can detect a URL that matches
   * both, which cannot identify a subject unambiguously.
   */
  @Nonnull
  private IBaseResource resolveCanonical(@Nonnull final String subjectCanonical) {
    final CanonicalReference canonical = CanonicalReference.parse(subjectCanonical);

    final List<IBaseResource> viewMatches = matchByUrl(VIEW_DEFINITION, canonical);
    final List<IBaseResource> libraryMatches = matchByUrl(LIBRARY, canonical);

    if (!viewMatches.isEmpty() && !libraryMatches.isEmpty()) {
      throw SqlOperationError.badRequest(
          IssueType.INVALID,
          SUBJECT_EXPRESSION,
          "The subject canonical '%s' is ambiguous: it matches both a ViewDefinition and a Library."
              .formatted(subjectCanonical));
    }
    if (!viewMatches.isEmpty()) {
      checkReadAuthority(VIEW_DEFINITION);
      return canonical.select(
          viewMatches, SubjectResolver::isActiveView, SubjectResolver::viewVersion);
    }
    if (!libraryMatches.isEmpty()) {
      checkReadAuthority(LIBRARY);
      return canonical.select(
          libraryMatches, SubjectResolver::isActiveLibrary, SubjectResolver::libraryVersion);
    }
    throw SqlOperationError.notFound(
        SUBJECT_EXPRESSION,
        "No ViewDefinition or Library matches the subject canonical '%s'."
            .formatted(subjectCanonical));
  }

  /**
   * Matches stored resources of the given type by the reference's url, and by its version when one
   * is pinned. A type with no stored data simply does not match, rather than surfacing the data
   * source's missing-type error.
   */
  @Nonnull
  private List<IBaseResource> matchByUrl(
      @Nonnull final String resourceType, @Nonnull final CanonicalReference canonical) {
    final Dataset<Row> all;
    try {
      all = dataSource.read(resourceType);
    } catch (final IllegalArgumentException e) {
      if (e.getMessage() != null && e.getMessage().contains("No data found for resource type")) {
        return List.of();
      }
      throw e;
    }
    Dataset<Row> filtered = all.filter(all.col("url").equalTo(canonical.getUrl()));
    if (canonical.hasVersion()) {
      filtered = filtered.filter(functions.col("version").equalTo(canonical.getVersion()));
    }
    final ExpressionEncoder<IBaseResource> encoder = fhirEncoders.of(resourceType);
    return filtered.as(encoder).collectAsList();
  }

  /**
   * Resolves a relative literal reference. The reference must name its resource type, since the two
   * artefact families are stored separately and a bare id cannot say which to read.
   */
  @Nonnull
  private IBaseResource resolveReference(@Nonnull final Reference subjectReference) {
    final String value = subjectReference.getReference();
    if (value == null || value.isBlank()) {
      throw SqlOperationError.badRequest(
          IssueType.INVALID,
          SUBJECT_EXPRESSION,
          "The subjectReference must carry a non-blank Reference.reference value.");
    }
    final int slash = value.indexOf('/');
    if (slash < 0) {
      throw SqlOperationError.badRequest(
          IssueType.INVALID,
          SUBJECT_EXPRESSION,
          "The subjectReference '%s' must name its resource type, as 'ViewDefinition/[id]' or"
                  .formatted(value)
              + " 'Library/[id]'.");
    }
    final String type = value.substring(0, slash);
    final String id = value.substring(slash + 1);
    if (!VIEW_DEFINITION.equals(type) && !LIBRARY.equals(type)) {
      throw SqlOperationError.badRequest(
          IssueType.INVALID,
          SUBJECT_EXPRESSION,
          "The subjectReference must point at a ViewDefinition or a Library, but found '%s'."
              .formatted(type));
    }
    if (id.isBlank()) {
      throw SqlOperationError.badRequest(
          IssueType.INVALID,
          SUBJECT_EXPRESSION,
          "The subjectReference '%s' is missing a logical id.".formatted(value));
    }

    checkReadAuthority(type);
    try {
      return readExecutor.read(type, id);
    } catch (final ResourceNotFoundError e) {
      throw SqlOperationError.notFound(
          SUBJECT_EXPRESSION, "No %s with id '%s' was found.".formatted(type, id));
    } catch (final IllegalArgumentException e) {
      if (e.getMessage() != null && e.getMessage().contains("No data found for resource type")) {
        throw SqlOperationError.notFound(
            SUBJECT_EXPRESSION, "No %s with id '%s' was found.".formatted(type, id));
      }
      throw e;
    }
  }

  /**
   * Enforces the resource-level READ check for an artefact read from storage, when authorisation is
   * enabled. This preserves the check the operations replaced by this feature applied to their
   * stored inputs.
   */
  private void checkReadAuthority(@Nonnull final String resourceType) {
    if (serverConfiguration.getAuth().isEnabled()) {
      SecurityAspect.checkHasAuthority(
          PathlingAuthority.resourceAccess(AccessType.READ, resourceType));
    }
  }

  private static boolean isActiveView(@Nonnull final IBaseResource resource) {
    final ViewDefinitionResource view = (ViewDefinitionResource) resource;
    return view.getStatusElement() != null
        && ACTIVE_STATUS.equals(view.getStatusElement().getValueAsString());
  }

  @Nullable
  private static String viewVersion(@Nonnull final IBaseResource resource) {
    return ((ViewDefinitionResource) resource).getVersion();
  }

  private static boolean isActiveLibrary(@Nonnull final IBaseResource resource) {
    return ((Library) resource).getStatus() == PublicationStatus.ACTIVE;
  }

  @Nullable
  private static String libraryVersion(@Nonnull final IBaseResource resource) {
    return ((Library) resource).getVersion();
  }
}
