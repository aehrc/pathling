package au.csiro.pathling.views;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;

public enum WhenMany {

  ERROR("error"),
  ARRAY("array"),
  UNNEST("unnest"),
  CROSS("cross");

  @Nonnull
  @Getter
  private final String code;

  WhenMany(@Nonnull final String code) {
    this.code = code;
  }

  public static WhenMany fromCode(@Nullable final String code) {
    for (final WhenMany whenMany : values()) {
      if (whenMany.code.equals(code)) {
        return whenMany;
      }
    }
    throw new IllegalArgumentException("Unknown code: " + code);
  }

}
