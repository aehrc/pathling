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

import au.csiro.pathling.views.FhirView;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.hl7.fhir.r4.model.Library;

/**
 * One entry of the repeating {@code context} parameter: a supporting artefact supplied inline for a
 * dependency the server cannot resolve, identified by the canonical URL it satisfies.
 *
 * <p>An entry is either a parsed {@code ViewDefinition}, which is a leaf of the dependency graph,
 * or a {@code SQLView} {@code Library}, whose own dependencies are traversed in turn so a chain of
 * supplied artefacts resolves.
 *
 * @author John Grimes
 */
public class SuppliedArtefact {

  @Nonnull private final String url;

  @Nullable private final String version;

  @Nullable private final FhirView view;

  @Nullable private final Library sqlView;

  private SuppliedArtefact(
      @Nonnull final String url,
      @Nullable final String version,
      @Nullable final FhirView view,
      @Nullable final Library sqlView) {
    this.url = url;
    this.version = version;
    this.view = view;
    this.sqlView = sqlView;
  }

  /**
   * Creates an entry backed by a supplied {@code ViewDefinition}.
   *
   * @param url the canonical URL the entry satisfies
   * @param version the entry's version, or null when it declares none
   * @param view the parsed view
   * @return the entry
   */
  @Nonnull
  public static SuppliedArtefact ofView(
      @Nonnull final String url, @Nullable final String version, @Nonnull final FhirView view) {
    return new SuppliedArtefact(url, version, view, null);
  }

  /**
   * Creates an entry backed by a supplied {@code SQLView} {@code Library}.
   *
   * @param url the canonical URL the entry satisfies
   * @param version the entry's version, or null when it declares none
   * @param sqlView the SQLView Library
   * @return the entry
   */
  @Nonnull
  public static SuppliedArtefact ofSqlView(
      @Nonnull final String url, @Nullable final String version, @Nonnull final Library sqlView) {
    return new SuppliedArtefact(url, version, null, sqlView);
  }

  /**
   * Returns the canonical URL this entry satisfies.
   *
   * @return the canonical URL
   */
  @Nonnull
  public String getUrl() {
    return url;
  }

  /**
   * Returns the entry's version, when it declares one.
   *
   * @return the version, or null
   */
  @Nullable
  public String getVersion() {
    return version;
  }

  /**
   * Indicates whether this entry is a supplied {@code ViewDefinition} rather than a {@code
   * SQLView}.
   *
   * @return true if the entry is a ViewDefinition
   */
  public boolean isView() {
    return view != null;
  }

  /**
   * Returns the parsed view backing this entry.
   *
   * @return the parsed view
   * @throws IllegalStateException if this entry is a SQLView rather than a ViewDefinition
   */
  @Nonnull
  public FhirView getView() {
    if (view == null) {
      throw new IllegalStateException(
          "Supplied artefact '%s' is not a ViewDefinition".formatted(url));
    }
    return view;
  }

  /**
   * Returns the SQLView Library backing this entry.
   *
   * @return the SQLView Library
   * @throws IllegalStateException if this entry is a ViewDefinition rather than a SQLView
   */
  @Nonnull
  public Library getSqlView() {
    if (sqlView == null) {
      throw new IllegalStateException("Supplied artefact '%s' is not a SQLView".formatted(url));
    }
    return sqlView;
  }

  /**
   * Indicates whether this entry may satisfy a dependency pinned to the given version. An entry
   * satisfies an unpinned reference regardless of its own version, but a pinned reference only when
   * the versions agree, so a request cannot silently substitute the wrong version of an artefact.
   *
   * @param referenceVersion the version the dependency reference pins, or null when unpinned
   * @return true if this entry may satisfy the reference
   */
  public boolean satisfiesVersion(@Nullable final String referenceVersion) {
    return referenceVersion == null || referenceVersion.equals(version);
  }
}
