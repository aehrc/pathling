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
}
