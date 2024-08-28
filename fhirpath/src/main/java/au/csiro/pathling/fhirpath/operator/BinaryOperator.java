/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.fhirpath.operator;

import au.csiro.pathling.fhirpath.collection.Collection;
import org.jetbrains.annotations.NotNull;

/**
 * Represents a binary operator in FHIRPath.
 *
 * @author John Grimes
 */
public interface BinaryOperator {

  /**
   * Invokes this operator with the specified inputs.
   *
   * @param input An {@link BinaryOperatorInput} object
   * @return A {@link Collection} object representing the resulting expression
   */
  @NotNull
  Collection invoke(BinaryOperatorInput input);

}
