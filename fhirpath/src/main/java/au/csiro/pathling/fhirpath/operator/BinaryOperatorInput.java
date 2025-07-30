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

import au.csiro.pathling.fhirpath.EvaluationContext;
import au.csiro.pathling.fhirpath.collection.Collection;
import jakarta.annotation.Nonnull;

/**
 * Represents the inputs to a binary operator in FHIRPath.
 *
 * @param context Context and dependencies for use in evaluating the function.
 * @param left An expression representing the left operand.
 * @param right An expression representing the right operand.
 * @author John Grimes
 */
public record BinaryOperatorInput(
    @Nonnull EvaluationContext context,
    @Nonnull Collection left,
    @Nonnull Collection right
) {

}
