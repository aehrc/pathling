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
}

/**
 * Represents a single filter input in the search form.
 */
export interface FilterInput {
  expression: string;
}
