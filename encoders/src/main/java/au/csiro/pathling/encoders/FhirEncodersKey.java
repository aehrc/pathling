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

package au.csiro.pathling.encoders;

import ca.uhn.fhir.context.FhirVersionEnum;
import jakarta.annotation.Nonnull;
import java.util.Set;
import lombok.Value;

/**
 * Immutable key to look up a matching encoders instance by configuration.
 */
@Value
public class FhirEncodersKey {

  /**
   * The FHIR version.
   */
  @Nonnull
  FhirVersionEnum fhirVersion;

  /**
   * The maximum nesting level for recursive types.
   */
  int maxNestingLevel;

  /**
   * The set of open types to encode.
   */
  @Nonnull
  Set<String> openTypes;

  /**
   * Whether extensions are enabled.
   */
  boolean enableExtensions;

}
