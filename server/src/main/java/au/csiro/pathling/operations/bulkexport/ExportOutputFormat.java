package au.csiro.pathling.operations.bulkexport;

import jakarta.annotation.Nonnull;
import org.jetbrains.annotations.Contract;

/**
 * Represents the output format for bulk export operations.
 *
 * @author Felix Naumann
 * @author John Grimes
 */
public enum ExportOutputFormat {
  /** Newline-delimited JSON format. */
  NDJSON;

  /**
   * Converts the export output format to its parameter string representation.
   *
   * @param exportOutputFormat the export output format to convert
   * @return the string representation used in export parameters
   */
  @Nonnull
  @Contract(pure = true)
  public static String asParam(@Nonnull final ExportOutputFormat exportOutputFormat) {
    return switch (exportOutputFormat) {
      case NDJSON -> "ndjson";
    };
  }
}
