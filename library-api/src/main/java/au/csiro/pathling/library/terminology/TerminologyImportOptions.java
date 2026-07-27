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

package au.csiro.pathling.library.terminology;

import au.csiro.pathling.terminology.store.DenseIdOrder;
import jakarta.annotation.Nullable;
import lombok.Builder;
import lombok.Value;

/**
 * Optional overrides for a terminology import. All options default to detection from the source, so
 * a null options object requests the default behaviour.
 *
 * @author John Grimes
 */
@Value
@Builder
public class TerminologyImportOptions {

  /**
   * Overrides the SNOMED CT edition/version URI when the RF2 release metadata is ambiguous. When
   * null, the edition and version are detected from the release's module and effectiveTime content.
   */
  @Nullable String editionUri;

  /**
   * Selects how dense identifiers are assigned to concepts. When null, they follow concept code
   * order, which is the behaviour of every release before this option existed. {@link
   * DenseIdOrder#PRE_ORDER} instead assigns them by a depth-first traversal of the is-a hierarchy,
   * which makes the runtime hierarchy index materially smaller but means identifiers shift whenever
   * the hierarchy changes shape between releases.
   */
  @Nullable DenseIdOrder denseIdOrder;
}
