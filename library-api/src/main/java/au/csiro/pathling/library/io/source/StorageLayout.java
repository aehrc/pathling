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
package au.csiro.pathling.library.io.source;

import jakarta.annotation.Nonnull;
import lombok.Getter;

/**
 * The layouts a stored resource table can be classified as, from its schema alone.
 *
 * <p>Only {@link #PARQUET_ON_FHIR} is the layout this release reads. {@link #UNCLASSIFIABLE} is
 * accepted as well, because a schema carrying no marker of either layout is not evidence of an
 * unsupported one (FR-039).
 *
 * @author Piotr Szul
 */
@Getter
public enum StorageLayout {
  /** The Parquet on FHIR layout, which this release writes and reads. */
  PARQUET_ON_FHIR("the Parquet on FHIR layout", true),

  /** The layout written by the encoder of earlier releases, which this release does not read. */
  PREVIOUS("the layout written by earlier releases of Pathling", false),

  /** A schema that carries no marker of either layout. */
  UNCLASSIFIABLE("an unclassifiable layout", true);

  /** A description of the layout, for use in messages. */
  @Nonnull private final String description;

  /** Whether data in this layout is accepted. */
  private final boolean supported;

  StorageLayout(@Nonnull final String description, final boolean supported) {
    this.description = description;
    this.supported = supported;
  }
}
