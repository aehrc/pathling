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
 * The result of translating a FHIR ValueSet compose (or expansion) to the local evaluation model:
 * the code system the value set evaluates over and the VCL expression that defines its members.
 *
 * @author John Grimes
 */
@Value
public class ComposeResult {

  /** The canonical URL of the code system the value set evaluates over. */
  @Nonnull String systemUrl;

  /** The membership rule to evaluate. */
  @Nonnull VclExpression expression;
}
