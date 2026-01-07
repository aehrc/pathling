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
