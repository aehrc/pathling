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

package au.csiro.pathling.fhirpath.evaluation;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import jakarta.annotation.Nonnull;
import lombok.Value;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Evaluator that combines a {@link SingleResourceEvaluator} with a {@link Dataset}, enabling
 * FHIRPath evaluation that returns {@link CollectionDataset} results.
 *
 * <p>This class bridges the gap between the Column-based evaluation provided by {@link
 * SingleResourceEvaluator} and Dataset-aware operations that need both the evaluation result and
 * the underlying data.
 *
 * <p>Use this evaluator when:
 *
 * <ul>
 *   <li>You need to evaluate FHIRPath expressions and get both the result columns and the dataset
 *   <li>You're working with test scenarios that need to verify expression results against data
 *   <li>You need {@link CollectionDataset} output for downstream operations like canonicalization
 * </ul>
 *
 * @see SingleResourceEvaluator
 * @see CollectionDataset
 * @see DatasetEvaluatorBuilder
 */
@Value
public class DatasetEvaluator {

  /** The underlying evaluator that performs FHIRPath expression evaluation. */
  @Nonnull SingleResourceEvaluator evaluator;

  /** The Spark Dataset containing the resource data to evaluate against. */
  @Nonnull Dataset<Row> dataset;

  /**
   * Evaluates a FHIRPath expression with the default input context.
   *
   * <p>The default input context is the subject resource, equivalent to calling {@link
   * #evaluate(FhirPath, Collection)} with the subject resource collection.
   *
   * @param fhirPath the FHIRPath expression to evaluate
   * @return the result of the evaluation paired with the dataset
   */
  @Nonnull
  public CollectionDataset evaluate(@Nonnull final FhirPath fhirPath) {
    final Collection result = evaluator.evaluate(fhirPath);
    return CollectionDataset.of(dataset, result);
  }

  /**
   * Evaluates a FHIRPath expression with a custom input context.
   *
   * <p>The input context is the collection that the FHIRPath expression operates on. This is
   * typically used when evaluating expressions in the context of a specific element rather than the
   * resource root.
   *
   * @param fhirPath the FHIRPath expression to evaluate
   * @param inputContext the input context collection to evaluate against
   * @return the result of the evaluation paired with the dataset
   */
  @Nonnull
  public CollectionDataset evaluate(
      @Nonnull final FhirPath fhirPath, @Nonnull final Collection inputContext) {
    final Collection result = evaluator.evaluate(fhirPath, inputContext);
    return CollectionDataset.of(dataset, result);
  }

  /**
   * Evaluates a FHIRPath expression and returns just the Collection result.
   *
   * <p>This method is useful when you need the evaluation result without the dataset, such as when
   * only the column expressions are needed.
   *
   * @param fhirPath the FHIRPath expression to evaluate
   * @return the evaluation result as a Collection
   */
  @Nonnull
  public Collection evaluateToCollection(@Nonnull final FhirPath fhirPath) {
    return evaluator.evaluate(fhirPath);
  }

  /**
   * Evaluates a FHIRPath expression and returns just the Collection result.
   *
   * <p>This method is useful when you need the evaluation result without the dataset, such as when
   * only the column expressions are needed.
   *
   * @param fhirPath the FHIRPath expression to evaluate
   * @param inputContext the input context collection to evaluate against
   * @return the evaluation result as a Collection
   */
  @Nonnull
  public Collection evaluateToCollection(
      @Nonnull final FhirPath fhirPath, @Nonnull final Collection inputContext) {
    return evaluator.evaluate(fhirPath, inputContext);
  }

  /**
   * Returns the subject resource code for this evaluator.
   *
   * <p>This method supports both standard FHIR resource types (e.g., "Patient", "Observation") and
   * custom resource types (e.g., "ViewDefinition").
   *
   * @return the subject resource type code
   */
  @Nonnull
  public String getSubjectResourceCode() {
    return evaluator.getSubjectResourceCode();
  }

  /**
   * Returns the default input context (subject resource collection).
   *
   * <p>This is the initial input context used when evaluating FHIRPath expressions that start from
   * the resource root.
   *
   * @return the subject resource collection
   */
  @Nonnull
  public ResourceCollection getDefaultInputContext() {
    return evaluator.getDefaultInputContext();
  }
}
