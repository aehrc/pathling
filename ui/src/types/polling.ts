/**
 * Common types for job polling operations.
 *
 * @author John Grimes
 */

/**
 * Result of polling a job's status.
 */
export interface PollResult<TManifest> {
  status: "in_progress" | "completed";
  progress?: number;
  manifest?: TManifest;
}
