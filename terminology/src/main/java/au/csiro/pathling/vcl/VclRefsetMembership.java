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

package au.csiro.pathling.vcl;

import jakarta.annotation.Nonnull;
import java.io.Serial;
import lombok.Value;

/**
 * Selects the members of a SNOMED CT reference set. This node is not part of the base VCL grammar,
 * which has no reference set operator; it is produced by translating the ECL reference set operator
 * ({@code ^ refsetId}) and by resolving the SNOMED implicit reference set value set form ({@code
 * ?fhir_vs=refset/[id]}). It evaluates to the concepts that are members of the reference set with
 * the given identifier.
 *
 * @author John Grimes
 */
@Value
public class VclRefsetMembership implements VclExpression {

  @Serial private static final long serialVersionUID = 1L;

  /** The reference set identifier whose members are selected. */
  @Nonnull String refsetCode;
}
