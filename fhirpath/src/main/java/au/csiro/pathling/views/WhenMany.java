package au.csiro.pathling.views;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;

public enum WhenMany {

  /**
   * Indicates that a variable should not have repeated values.
   */
  ERROR("error"),

  /**
   * Indicates that a variable should create an array for repeated values.
   */
  ARRAY("array"),

  /**
   * Indicates that repeated values should be unnested into separate rows. Does not allow for
   * multiple levels of nesting that have the root resource as their nearest common ancestor.
   */
  UNNEST("unnest");

  @Nonnull
  @Getter
  private final String code;

  WhenMany(@Nonnull final String code) {
    this.code = code;
  }

  /**
   * Returns the {@link WhenMany} value corresponding to the given code.
   *
   * @param code the code to look up
   * @return the corresponding {@link WhenMany} value
   */
  public static WhenMany fromCode(@Nullable final String code) {
    for (final WhenMany whenMany : values()) {
      if (whenMany.code.equals(code)) {
        return whenMany;
      }
    }
    throw new IllegalArgumentException("Unknown code: " + code);
  }

}
