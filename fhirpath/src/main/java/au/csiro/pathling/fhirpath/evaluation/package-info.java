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

/**
 * Provides simplified FHIRPath evaluation infrastructure for single-resource scenarios.
 *
 * <p>This package contains classes for evaluating FHIRPath expressions against a single FHIR
 * resource type without requiring a {@link au.csiro.pathling.io.source.DataSource}. The evaluator
 * produces {@link au.csiro.pathling.fhirpath.collection.Collection} objects containing Spark SQL
 * Column expressions that can be applied to datasets.
 *
 * <h2>Key Classes</h2>
 *
 * <ul>
 *   <li>{@link au.csiro.pathling.fhirpath.evaluation.SingleResourceEvaluator} - Main evaluator
 *       class
 *   <li>{@link au.csiro.pathling.fhirpath.evaluation.SingleResourceEvaluatorBuilder} - Builder for
 *       creating evaluator instances
 *   <li>{@link au.csiro.pathling.fhirpath.evaluation.CrossResourceStrategy} - Defines how
 *       cross-resource references are handled
 * </ul>
 *
 * <h2>Usage Example</h2>
 *
 * <pre>{@code
 * // Create evaluator for single-resource evaluation
 * SingleResourceEvaluator evaluator = SingleResourceEvaluatorBuilder
 *     .create(ResourceType.PATIENT, fhirContext)
 *     .withCrossResourceStrategy(CrossResourceStrategy.EMPTY)
 *     .build();
 *
 * // Evaluate FHIRPath expression
 * FhirPath fhirPath = parser.parse("name.family");
 * Collection result = evaluator.evaluate(fhirPath);
 *
 * // Get the Column for filtering
 * Column filter = result.getColumn().getValue();
 * }</pre>
 *
 * @see au.csiro.pathling.fhirpath.evaluation.SingleResourceEvaluator
 * @see au.csiro.pathling.fhirpath.evaluation.SingleResourceEvaluatorBuilder
 */
package au.csiro.pathling.fhirpath.evaluation;
