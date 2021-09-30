/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.caching;

import java.util.Date;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * Maintains a simple state based upon the last time at which data was updated, and provides
 * operations for generating and validating ETags against this state.
 *
 * @author John Grimes
 */
@Component
@Profile("server")
public class EntityTagValidator {

  private long updatedAt;

  /**
   * Creates a new instance.
   */
  public EntityTagValidator() {
    updatedAt = currentTime();
  }

  /**
   * Creates a new instance.
   *
   * @param updatedAt an initial value for the updated at state
   */
  public EntityTagValidator(final long updatedAt) {
    this.updatedAt = updatedAt;
  }

  /**
   * @return a new ETag value based on the current state
   */
  @Nonnull
  public String tag() {
    return tagForTime(updatedAt);
  }

  /**
   * @param time a time to use as the basis for the tag
   * @return a new ETag value based on the current time
   */
  @Nonnull
  public String tagForTime(final long time) {
    return "W/\"" + Long.toString(time, Character.MAX_RADIX) + "\"";
  }

  /**
   * @param tag the tag to be validated
   * @return true if the tag matches the current state
   */
  public boolean matches(@Nullable final String tag) {
    if (tag == null) {
      return false;
    }
    return tag.equals(tag());
  }

  /**
   * Reset the current state.
   *
   * @param resetTime the time to reset the state to
   */
  public void expire(final long resetTime) {
    updatedAt = resetTime;
  }

  private long currentTime() {
    return new Date().getTime();
  }

}
