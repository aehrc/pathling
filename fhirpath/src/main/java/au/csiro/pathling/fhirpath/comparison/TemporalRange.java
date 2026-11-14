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

package au.csiro.pathling.fhirpath.comparison;

import jakarta.annotation.Nonnull;
import lombok.Value;
import org.apache.spark.sql.Column;

/** The lower and upper bounds of one operand of a temporal comparison, as it reads them. */
@Value
class TemporalRange {

  /** The lower bound. */
  @Nonnull Column start;

  /** The upper bound. */
  @Nonnull Column end;
}
