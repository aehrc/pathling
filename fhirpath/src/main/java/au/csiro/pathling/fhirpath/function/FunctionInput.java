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

package au.csiro.pathling.fhirpath.function;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.evaluation.EvaluationContext;
import java.util.List;
import org.jetbrains.annotations.NotNull;

/**
 * Represents the inputs to a FHIRPath function.
 *
 * @param context Context and dependencies for use in evaluating the function.
 * @param input The collection that is the input to the function, i.e. the result of the evaluation
 * of the expression on the left-hand side of the dot preceding the function invocation.
 * @param arguments A list of expressions representing the arguments to the function, i.e. the
 * expressions inside the parentheses following the function invocation, separated by commas.
 * @author John Grimes
 */
public record FunctionInput(@NotNull EvaluationContext context, @NotNull Collection input,
                            @NotNull List<FhirPath> arguments) {

}
