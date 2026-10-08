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

package au.csiro.pathling.io;

import jakarta.annotation.Nonnull;

/**
 * Selects a format by the media type that names it, which is how the library's encoding entry
 * points name formats (decision 84).
 */
final class FhirMediaType {

  /** The media type of FHIR JSON. */
  @Nonnull static final String JSON = "application/fhir+json";

  /** The media type of FHIR XML. */
  @Nonnull static final String XML = "application/fhir+xml";

  private FhirMediaType() {}

  /**
   * Returns whichever of two values belongs to the format a media type names.
   *
   * @param mediaType the media type
   * @param json the value for FHIR JSON
   * @param xml the value for FHIR XML
   * @param <T> the type of the values
   * @return the value for the format named
   * @throws IllegalArgumentException if the media type names neither format
   */
  @Nonnull
  static <T> T select(
      @Nonnull final String mediaType, @Nonnull final T json, @Nonnull final T xml) {
    return switch (mediaType) {
      case JSON -> json;
      case XML -> xml;
      default -> throw new IllegalArgumentException("Unsupported media type: " + mediaType);
    };
  }
}
