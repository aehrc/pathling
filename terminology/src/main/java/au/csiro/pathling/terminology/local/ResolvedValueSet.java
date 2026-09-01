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

package au.csiro.pathling.terminology.local;

import au.csiro.pathling.vcl.VclExpression;
import jakarta.annotation.Nonnull;
import lombok.Value;

/**
 * The outcome of resolving a value set URL against the store: the code system version the value set
 * evaluates over and the VCL expression that defines its members. It is produced only when the
 * referenced content is present in the store; an absent reference yields no resolution and the
 * unknown-content fallback applies.
 *
 * @author John Grimes
 */
@Value
public class ResolvedValueSet {

  /** The stable identifier of the code system version the value set evaluates over. */
  @Nonnull String systemVersionId;

  /** The canonical URL of the code system the value set evaluates over. */
  @Nonnull String systemUrl;

  /** The membership rule to evaluate. */
  @Nonnull VclExpression expression;
}
