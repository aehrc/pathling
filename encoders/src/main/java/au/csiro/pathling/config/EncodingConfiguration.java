/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright 2018-2025 Commonwealth Scientific and Industrial Research
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
 *
 */

package au.csiro.pathling.config;

import au.csiro.pathling.encoders.FhirEncoders;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import java.util.Set;
import lombok.Builder;
import lombok.Data;

/**
 * Represents configuration specific to FHIR encoding.
 */
@Data
@Builder
public class EncodingConfiguration {

  /**
   * Controls the maximum depth of nested element data that is encoded upon import.
   */
  @NotNull
  @Min(0)
  @Builder.Default
  private Integer maxNestingLevel = 3;

  /**
   * Enables support for FHIR extensions.
   */
  @NotNull
  @Builder.Default
  private boolean enableExtensions = true;

  /**
   * The list of types that are encoded within open types, such as extensions. This default list was
   * taken from the data types that are common to extensions found in widely-used IGs, such as the
   * US and AU base profiles. In general, you will get the best query performance by encoding your
   * data with the shortest possible list.
   */
  @NotNull
  @Builder.Default
  private Set<String> openTypes = FhirEncoders.STANDARD_OPEN_TYPES;
}
