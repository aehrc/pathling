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
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.security.PathlingAuthority;
import au.csiro.pathling.security.ResourceAccess.AccessType;
import au.csiro.pathling.security.SecurityAspect;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.functions;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations.PublicationStatus;
import org.hl7.fhir.r4.model.Library;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Resolves a canonical reference such as {@code https://example.org/Library/foo} or {@code
 * https://example.org/Library/foo|1.2} to a stored {@code SQLView} {@code Library}, matched against
 * {@code Library.url}.
 *
 * @author John Grimes
 */
@Component
public class LibraryReferenceResolver {

  private static final String LIBRARY = "Library";

  @Nonnull private final DataSource dataSource;

  @Nonnull private final FhirEncoders fhirEncoders;

  @Nonnull private final ServerConfiguration serverConfiguration;

  /**
   * Constructs a new LibraryReferenceResolver.
   *
   * @param dataSource the data source searched for matching Libraries
   * @param fhirEncoders FHIR encoders used to decode the search result rows
   * @param serverConfiguration the server configuration (used for the auth toggle)
   */
  @Autowired
  public LibraryReferenceResolver(
      @Nonnull final DataSource dataSource,
      @Nonnull final FhirEncoders fhirEncoders,
      @Nonnull final ServerConfiguration serverConfiguration) {
    this.dataSource = dataSource;
    this.fhirEncoders = fhirEncoders;
    this.serverConfiguration = serverConfiguration;
  }

  /**
   * Attempts to resolve a canonical reference to a stored {@code SQLView} {@code Library} by
   * matching {@code Library.url}, returning empty when no Library matches. Used by {@link
   * SqlDependencyResolver} for the SQLView arm of canonical dependency resolution, where a
   * non-match is not an error in itself (the reference may instead name a {@code ViewDefinition}).
   *
   * <p>The url/version split and candidate selection (an exact {@code url|version} match, else the
   * latest active version) are delegated to the shared {@link CanonicalReference} helper, so a
   * {@code Library} and a {@code ViewDefinition} are selected by identical rules. The {@code
   * Library} metadata READ check is enforced (when authorisation is enabled) only once a Library is
   * actually matched, so referencing a URL that names a {@code ViewDefinition} the caller can read
   * is not blocked by missing {@code Library} authority.
   *
   * @param canonical the canonical reference to resolve
   * @return the resolved Library, or empty if no Library matches the canonical url
   */
  @Nonnull
  public Optional<Library> tryResolveSqlViewLibrary(@Nonnull final String canonical) {
    final CanonicalReference reference = CanonicalReference.parse(canonical);

    final Dataset<Row> libraries;
    try {
      libraries = dataSource.read(LIBRARY);
    } catch (final IllegalArgumentException e) {
      // The server holds no Library data at all, so the reference simply does not match a SQLView.
      if (e.getMessage() != null && e.getMessage().contains("No data found for resource type")) {
        return Optional.empty();
      }
      throw e;
    }
    Dataset<Row> filtered = libraries.filter(libraries.col("url").equalTo(reference.getUrl()));
    if (reference.hasVersion()) {
      filtered = filtered.filter(functions.col("version").equalTo(reference.getVersion()));
    }

    final ExpressionEncoder<IBaseResource> encoder = fhirEncoders.of(LIBRARY);
    final List<IBaseResource> candidates = filtered.as(encoder).collectAsList();
    if (candidates.isEmpty()) {
      return Optional.empty();
    }

    if (serverConfiguration.getAuth().isEnabled()) {
      SecurityAspect.checkHasAuthority(PathlingAuthority.resourceAccess(AccessType.READ, LIBRARY));
    }
    final IBaseResource selected =
        reference.select(
            candidates,
            candidate -> ((Library) candidate).getStatus() == PublicationStatus.ACTIVE,
            candidate -> ((Library) candidate).getVersion());
    return Optional.of((Library) selected);
  }
}
