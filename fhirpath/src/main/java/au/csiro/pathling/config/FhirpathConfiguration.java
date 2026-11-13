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

package au.csiro.pathling.config;

import jakarta.annotation.Nonnull;
import jakarta.validation.constraints.Min;
import lombok.Builder;
import lombok.Data;

/**
 * Configuration parameters for FHIRPath expression evaluation.
 *
 * @author Piotr Szul
 */
@Data
@Builder
public class FhirpathConfiguration {

  /** The default configuration instance. */
  @Nonnull
  public static final FhirpathConfiguration DEFAULT = FhirpathConfiguration.builder().build();

  /**
   * Maximum depth for same-type recursive traversal in {@code repeat()} and {@code repeatAll()}.
   * Cross-type traversals do not consume depth budget.
   */
  @Min(1)
  @Builder.Default
  private int maxUnboundTraversalDepth = 10;

  /**
   * Whether a primitive's id and extensions are read from the metadata group stored beside it
   * (decision 141, D14). Off is the default.
   *
   * <p>On, every FHIR primitive reached by traversal is held as one structure of its value, its
   * {@code id} and {@code extension}, and the annotations the schema carries (decision 152), so
   * that {@code x.id} and {@code x.extension(...)} on a primitive return what was stored. Off, a
   * primitive carries its value and annotations only, and those navigations are empty, with no
   * error, as if the source had carried no id or extensions. This is separate from the ingest
   * option that stores them, so that data stored with them can be read without them.
   */
  @Builder.Default private boolean readPrimitiveMetadata = false;
}
