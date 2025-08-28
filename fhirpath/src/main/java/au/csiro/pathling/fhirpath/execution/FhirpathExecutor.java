/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.fhirpath.execution;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.parser.Parser;
import jakarta.annotation.Nonnull;
import java.util.List;
import lombok.Value;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Executes FHIRPath expressions by parsing and evaluating them against FHIR resources.
 * <p>
 * This class provides a high-level interface for executing FHIRPath expressions as strings,
 * handling the parsing and evaluation process. It combines a {@link Parser} for converting
 * string expressions into {@link FhirPath} objects and a {@link FhirpathEvaluator.Provider}
 * for creating evaluators that can execute those expressions against resources.
 * <p>
 * The executor is designed to be reused for multiple evaluations, potentially against
 * different resource types, while maintaining the same parsing and evaluation configuration.
 */
@Value(staticConstructor = "of")
public class FhirpathExecutor {

  /**
   * The parser used to convert string expressions into {@link FhirPath} objects.
   */
  @Nonnull
  Parser parser;

  /**
   * The provider used to create {@link FhirpathEvaluator} instances for evaluation.
   */
  @Nonnull
  FhirpathEvaluator.Provider provider;

  /**
   * Evaluates a FHIRPath expression against a specific resource type.
   * <p>
   * This method:
   * <ol>
   *   <li>Parses the string expression into a {@link FhirPath} object</li>
   *   <li>Creates an evaluator for the specified resource type</li>
   *   <li>Evaluates the expression using that evaluator</li>
   *   <li>Returns the result as a {@link CollectionDataset} containing both the
   *       initial dataset and the evaluation result</li>
   * </ol>
   *
   * @param subjectResource the resource type to evaluate against (e.g., Patient, Observation)
   * @param fhirpathExpression the FHIRPath expression as a string (e.g., "name.given")
   * @return a CollectionDataset containing the initial dataset and the evaluation result
   */
  @Nonnull
  public CollectionDataset evaluate(@Nonnull final ResourceType subjectResource,
      @Nonnull final String fhirpathExpression) {
    final FhirPath fhirpath = parser.parse(fhirpathExpression);
    final FhirpathEvaluator evaluator = provider.create(subjectResource, () -> List.of(fhirpath));
    return CollectionDataset.of(evaluator.createInitialDataset(), evaluator.evaluate(fhirpath));
  }
}
