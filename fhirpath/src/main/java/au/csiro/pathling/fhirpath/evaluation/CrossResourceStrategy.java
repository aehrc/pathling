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

/**
 * Defines the strategy for handling cross-resource references during single-resource FHIRPath
 * evaluation.
 * <p>
 * When evaluating FHIRPath expressions against a single resource type, references to other
 * resource types (e.g., {@code Observation.subject.resolve()}) cannot be resolved. This enum
 * defines how such situations should be handled.
 */
public enum CrossResourceStrategy {

  /**
   * Throw an exception when cross-resource references are encountered.
   * <p>
   * This is the default and strictest strategy. Use this when cross-resource references
   * are unexpected and should be treated as errors.
   */
  FAIL,

  /**
   * Treat foreign resources as empty collections with correct type information but null values.
   * <p>
   * This strategy allows FHIRPath expressions that reference other resources to evaluate
   * gracefully by returning empty results for foreign resource paths. The returned collection
   * will have the correct type information (allowing type checking to work) but will contain
   * {@code lit(null)} as its column value.
   * <p>
   * This is useful for:
   * <ul>
   *   <li>Search parameter evaluation where cross-resource paths may exist but are optional</li>
   *   <li>Filter expression building where null results are acceptable</li>
   * </ul>
   */
  EMPTY
}
