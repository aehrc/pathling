package au.csiro.pathling.operations.bulkimport;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import lombok.Getter;

/**
 * Supported import modes for Pathling persistence schemes.
 *
 * @author John Grimes
 */
public enum ImportMode {
  /**
   * Results in all existing resources of the specified type to be deleted and replaced with the
   * contents of the source file.
   */
  OVERWRITE("overwrite"),

  /**
   * Matches existing resources with updated resources in the source file based on their ID, and
   * either update the existing resources or add new resources as appropriate.
   */
  MERGE("merge");

  @Nonnull
  @Getter
  private final String code;

  ImportMode(@Nonnull final String code) {
    this.code = code;
  }

  @Nonnull
  public static ImportMode fromCode(@Nullable final String code) {
    for (final ImportMode mode : values()) {
      if (mode.code.equals(code)) {
        return mode;
      }
    }
    throw new IllegalArgumentException("Unknown import mode: " + code);
  }
}
