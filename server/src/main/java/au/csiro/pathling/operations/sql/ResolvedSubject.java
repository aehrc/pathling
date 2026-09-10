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

import au.csiro.pathling.encoders.ViewDefinitionResource;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Library;

/**
 * A subject of {@code $sql-run} or {@code $sql-export} after its naming form has been resolved: the
 * artefact itself, the kind it conforms to, and the output name supplied alongside it (on {@code
 * $sql-export}).
 *
 * <p>The artefact is kept in its FHIR form rather than parsed, because the two kinds are handed to
 * different evaluation engines: a {@code ViewDefinition} is parsed into a {@code FhirView}, while a
 * SQL Library goes to the SQL pipeline whole.
 *
 * @author John Grimes
 */
public class ResolvedSubject {

  @Nonnull private final SubjectKind kind;

  @Nonnull private final IBaseResource resource;

  @Nullable private final String suppliedName;

  /**
   * Constructs a new ResolvedSubject.
   *
   * @param kind the kind the artefact conforms to
   * @param resource the resolved artefact, a ViewDefinition or a SQL Library
   * @param suppliedName the {@code subject.name} supplied at kick-off, or null when none was
   */
  public ResolvedSubject(
      @Nonnull final SubjectKind kind,
      @Nonnull final IBaseResource resource,
      @Nullable final String suppliedName) {
    this.kind = kind;
    this.resource = resource;
    this.suppliedName = suppliedName;
  }

  /**
   * Returns the kind the artefact conforms to.
   *
   * @return the subject kind
   */
  @Nonnull
  public SubjectKind getKind() {
    return kind;
  }

  /**
   * Returns the resolved artefact in its FHIR form.
   *
   * @return the ViewDefinition or SQL Library resource
   */
  @Nonnull
  public IBaseResource getResource() {
    return resource;
  }

  /**
   * Returns the artefact as a {@code Library}, for the two SQL kinds.
   *
   * @return the Library resource
   * @throws IllegalStateException if this subject is not one of the SQL kinds
   */
  @Nonnull
  public Library asLibrary() {
    if (resource instanceof final Library library) {
      return library;
    }
    throw new IllegalStateException("Subject of kind %s is not a Library resource".formatted(kind));
  }

  /**
   * Derives the output name for this subject: the supplied {@code name} when present, else the
   * artefact's own {@code name} element, else a generated identifier based on the subject's
   * position in the request.
   *
   * @param index the zero-based position of this subject in the request, used for the generated
   *     fallback
   * @return the effective output name, before cross-job uniqueness is checked
   */
  @Nonnull
  public String getEffectiveName(final int index) {
    if (suppliedName != null && !suppliedName.isBlank()) {
      return suppliedName;
    }
    final String artefactName = artefactName();
    if (artefactName != null && !artefactName.isBlank()) {
      return artefactName;
    }
    return "subject_" + index;
  }

  /**
   * Returns the artefact's own {@code name} element, or null when it declares none.
   *
   * @return the artefact name, or null
   */
  @Nullable
  public String artefactName() {
    if (resource instanceof final ViewDefinitionResource viewDefinition) {
      return viewDefinition.getNameElement() == null
          ? null
          : viewDefinition.getNameElement().getValue();
    }
    if (resource instanceof final Library library) {
      return library.hasName() ? library.getName() : null;
    }
    return null;
  }
}
