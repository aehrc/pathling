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

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.errors.ResourceNotFoundError;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.read.ReadExecutor;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.functions;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations.PublicationStatus;
import org.hl7.fhir.r4.model.Library;
import org.hl7.fhir.r4.model.Reference;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Resolves a {@link Reference} pointing at a stored {@code Library} resource. Supports both forms
 * defined by FHIR References:
 *
 * <ul>
 *   <li>Relative literal references such as {@code Library/abc} (or a bare id).
 *   <li>Canonical references such as {@code https://example.org/Library/foo} or {@code
 *       https://example.org/Library/foo|1.2}, matched against {@code Library.url}.
 * </ul>
 */
@Component
public class LibraryReferenceResolver {

  private static final String LIBRARY = "Library";

  @Nonnull private final ReadExecutor readExecutor;

  @Nonnull private final DataSource dataSource;

  @Nonnull private final FhirEncoders fhirEncoders;

  /**
   * Constructs a new LibraryReferenceResolver.
   *
   * @param readExecutor used for relative-reference reads
   * @param dataSource the data source used for canonical-reference search
   * @param fhirEncoders FHIR encoders used to decode the search result rows
   */
  @Autowired
  public LibraryReferenceResolver(
      @Nonnull final ReadExecutor readExecutor,
      @Nonnull final DataSource dataSource,
      @Nonnull final FhirEncoders fhirEncoders) {
    this.readExecutor = readExecutor;
    this.dataSource = dataSource;
    this.fhirEncoders = fhirEncoders;
  }

  /**
   * Resolves the reference to a stored Library resource.
   *
   * @param reference the reference to resolve; must carry a non-blank {@code reference} value
   * @return the resolved Library resource
   * @throws InvalidRequestException if the reference is malformed
   * @throws ResourceNotFoundException if no Library matches the reference
   */
  @Nonnull
  public IBaseResource resolve(@Nonnull final Reference reference) {
    final String ref = reference.getReference();
    if (ref == null || ref.isBlank()) {
      throw new InvalidRequestException(
          "queryReference must carry a non-blank Reference.reference value");
    }

    if (isCanonical(ref)) {
      return resolveCanonical(ref);
    }
    return resolveRelative(ref);
  }

  private boolean isCanonical(@Nonnull final String ref) {
    return ref.startsWith("http://")
        || ref.startsWith("https://")
        || ref.startsWith("urn:")
        || ref.contains("|");
  }

  /**
   * Resolves a relative literal reference. Accepts {@code Library/abc} or a bare id; anything that
   * names a different resource type is rejected.
   */
  @Nonnull
  private IBaseResource resolveRelative(@Nonnull final String ref) {
    final String id;
    if (ref.contains("/")) {
      final int slash = ref.indexOf('/');
      final String type = ref.substring(0, slash);
      if (!LIBRARY.equals(type)) {
        throw new InvalidRequestException(
            "queryReference must point at a Library resource, but found '" + type + "'");
      }
      id = ref.substring(slash + 1);
    } else {
      id = ref;
    }

    if (id.isBlank()) {
      throw new InvalidRequestException("queryReference is missing a Library id");
    }

    try {
      return readExecutor.read(LIBRARY, id);
    } catch (final ResourceNotFoundError e) {
      throw new ResourceNotFoundException("Library with ID '" + id + "' not found");
    } catch (final IllegalArgumentException e) {
      if (e.getMessage() != null && e.getMessage().contains("No data found for resource type")) {
        throw new ResourceNotFoundException("Library with ID '" + id + "' not found");
      }
      throw e;
    }
  }

  /**
   * Resolves a canonical reference of the form {@code [url]} or {@code [url]|[version]}. When
   * multiple matches exist, prefers an exact {@code url|version} match, then the latest active
   * version (by {@code Library.version} string ordering, since FHIR doesn't constrain its shape).
   */
  @Nonnull
  private IBaseResource resolveCanonical(@Nonnull final String canonical) {
    final int pipe = canonical.indexOf('|');
    final String url = pipe >= 0 ? canonical.substring(0, pipe) : canonical;
    final String version = pipe >= 0 ? canonical.substring(pipe + 1) : null;

    if (url.isBlank()) {
      throw new InvalidRequestException("queryReference canonical is missing the url segment");
    }

    final Dataset<Row> libraries = dataSource.read(LIBRARY);
    Dataset<Row> filtered = libraries.filter(libraries.col("url").equalTo(url));
    if (version != null && !version.isBlank()) {
      filtered = filtered.filter(functions.col("version").equalTo(version));
    }

    final ExpressionEncoder<IBaseResource> encoder = fhirEncoders.of(LIBRARY);
    final List<IBaseResource> candidates = filtered.as(encoder).collectAsList();
    if (candidates.isEmpty()) {
      throw new ResourceNotFoundException(
          "Library with canonical reference '" + canonical + "' not found");
    }
    return pickBestCandidate(candidates, version);
  }

  /**
   * Selects the most appropriate Library when multiple match the canonical url. With a version
   * suffix all matches already share that version, so any candidate suffices. Without a version
   * suffix, prefer active over draft/retired, then take the lexicographically greatest version
   * string (a reasonable proxy for "latest" given FHIR's freeform versioning).
   */
  @Nonnull
  private IBaseResource pickBestCandidate(
      @Nonnull final List<IBaseResource> candidates, @Nullable final String version) {
    if (candidates.size() == 1 || version != null) {
      return candidates.get(0);
    }
    return candidates.stream()
        .map(Library.class::cast)
        .max(
            Comparator.comparing((Library lib) -> lib.getStatus() == PublicationStatus.ACTIVE)
                .thenComparing(lib -> Objects.toString(lib.getVersion(), "")))
        .map(IBaseResource.class::cast)
        .orElseThrow(
            () ->
                new ResourceNotFoundException(
                    "Library with canonical reference could not be selected"));
  }
}
