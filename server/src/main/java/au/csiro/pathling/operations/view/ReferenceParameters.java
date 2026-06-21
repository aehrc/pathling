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

import jakarta.annotation.Nullable;
import org.hl7.fhir.instance.model.api.IIdType;
import org.hl7.fhir.r4.model.Reference;

/**
 * Helper for reducing a FHIR {@link Reference} parameter to the logical id of the stored resource
 * it points at, for the SQL on FHIR run and export operations.
 *
 * <p>Only literal relative references of the form {@code ResourceType/id} (and a bare {@code id})
 * are supported. Canonical, absolute, contained, and logical references are out of scope and are
 * treated as unresolvable.
 *
 * @author John Grimes
 */
final class ReferenceParameters {

  private ReferenceParameters() {
    // Utility class; not intended to be instantiated.
  }

  /**
   * Extracts the logical id from a literal relative {@link Reference}.
   *
   * @param reference the reference to reduce, may be {@code null}
   * @return the logical id (e.g. {@code 123} from {@code Patient/123}, or {@code abc} from a bare
   *     {@code abc}), or {@code null} when the reference is empty, absolute, or otherwise not a
   *     resolvable relative reference
   */
  @Nullable
  static String extractId(@Nullable final Reference reference) {
    if (reference == null) {
      return null;
    }
    final String literal = reference.getReference();
    if (literal == null || literal.isBlank()) {
      return null;
    }
    final IIdType element = reference.getReferenceElement();
    // Absolute and canonical URLs are out of scope; only relative references resolve.
    if (element.isAbsolute()) {
      return null;
    }
    final String idPart = element.getIdPart();
    return (idPart == null || idPart.isBlank()) ? null : idPart;
  }
}
