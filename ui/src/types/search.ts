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

/**
 * Type definitions for FHIR resource search operations.
 *
 * @author John Grimes
 */

/**
 * Request parameters for a FHIR search operation.
 */
export interface SearchRequest {
  resourceType: string;
  filters: string[];
  /** Standard FHIR search parameters as name-value pairs. */
  params: Record<string, string[]>;
}

/**
 * Represents a single filter input in the search form.
 */
export interface FilterInput {
  expression: string;
}
