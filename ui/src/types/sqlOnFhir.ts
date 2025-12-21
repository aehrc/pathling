/**
 * Type definitions for SQL on FHIR operations.
 *
 * @author John Grimes
 */

/**
 * Summary information for a ViewDefinition resource.
 */
export interface ViewDefinitionSummary {
  id: string;
  name: string;
  json: string;
}

/**
 * Request to execute a ViewDefinition.
 */
export interface ViewDefinitionExecuteRequest {
  mode: "stored" | "inline";
  viewDefinitionId?: string;
  viewDefinitionJson?: string;
}

/**
 * Result of executing a ViewDefinition.
 */
export interface ViewDefinitionResult {
  columns: string[];
  rows: Record<string, unknown>[];
}
