/**
 * Utility function for formatting dates and times.
 *
 * @author John Grimes
 */

/**
 * Formats a date as a date time string (e.g., "7 Jan 2026, 10:30:45 AM").
 *
 * @param date - The date to format.
 * @returns The formatted date time string.
 */
export function formatDateTime(date: Date): string {
  return date.toLocaleString([], {
    day: "numeric",
    month: "short",
    year: "numeric",
    hour: "numeric",
    minute: "2-digit",
    second: "2-digit",
  });
}
