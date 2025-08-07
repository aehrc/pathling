package au.csiro.pathling.library.io;

import jakarta.annotation.Nonnull;
import java.util.Optional;
import lombok.Getter;

/**
 * Save modes supported when writing data to a data sink.
 *
 * @author John Grimes
 */
public enum SaveMode {
  /**
   * If the resource type already exists, an error is raised. If it does not exist, the resources in
   * the source file are added.
   */
  ERROR_IF_EXISTS("error", Optional.of(org.apache.spark.sql.SaveMode.ErrorIfExists)),

  /**
   * Results in all existing resources of the specified type to be deleted and replaced with the
   * contents of the source file.
   */
  OVERWRITE("overwrite", Optional.of(org.apache.spark.sql.SaveMode.Overwrite)),

  /**
   * Appends the resources in the source file to the existing resources of the specified type,
   * without modifying any existing resources.
   */
  APPEND("append", Optional.of(org.apache.spark.sql.SaveMode.Append)),

  /**
   * Ignores the resources in the source file if the resource type already exists, otherwise adds
   * the resources in the source file.
   */
  IGNORE("ignore", Optional.of(org.apache.spark.sql.SaveMode.Ignore)),

  /**
   * Matches existing resources with updated resources in the source file based on their ID, and
   * either update the existing resources or add new resources as appropriate.
   */
  MERGE("merge", Optional.empty());

  @Nonnull
  @Getter
  private final String code;

  @Nonnull
  @Getter
  private final Optional<org.apache.spark.sql.SaveMode> sparkSaveMode;

  SaveMode(@Nonnull final String code, @Nonnull final Optional<org.apache.spark.sql.SaveMode> sparkSaveMode) {
    this.code = code;
    this.sparkSaveMode = sparkSaveMode;
  }

  /**
   * Returns the SaveMode corresponding to the given code.
   *
   * @param code the string code to convert
   * @return the corresponding SaveMode
   * @throws IllegalArgumentException if the code is not recognised
   */
  @Nonnull
  public static SaveMode fromCode(final String code) {
    for (final SaveMode mode : values()) {
      if (mode.code.equals(code)) {
        return mode;
      }
    }
    throw new IllegalArgumentException("Unknown import mode: " + code);
  }

}
