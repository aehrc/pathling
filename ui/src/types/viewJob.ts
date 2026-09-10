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
 * Type definitions for SQL on FHIR view jobs.
 *
 * @author John Grimes
 */

export type ViewJobMode = "stored" | "inline";

/**
 * Represents an active or completed view query job for tracking in the UI.
 */
export interface ViewJob {
  id: string;
  mode: ViewJobMode;
  viewDefinitionId?: string;
  viewDefinitionJson?: string;
  limit?: number;
  createdAt: Date;
}

/**
 * A request to run a ViewDefinition, as captured by the authoring form.
 */
export interface ViewRunRequest {
  /** Whether the view is stored on the server or supplied inline. */
  mode: ViewJobMode;
  /** Id of a stored ViewDefinition, when the mode is stored. */
  viewDefinitionId?: string;
  /** JSON of an inline ViewDefinition, when the mode is inline. */
  viewDefinitionJson?: string;
  /** Maximum rows to return. */
  limit?: number;
}
