/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.caching;

import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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

  private static final Pattern ETAG = Pattern.compile("W/\"(.*)\"");

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
   * @param tag the tag to be validated
   * @param expiryPeriod an expiry period, which is checked against the time encoded within the tag
   * @param comparisonTime the time to compare the tag to, for the purposes of expiry
   * @return true if the tag is valid when compared against the state, and also accounting for
   * expiry
   */
  public boolean validWithExpiry(@Nullable final CharSequence tag, final long expiryPeriod,
      final long comparisonTime) {
    if (tag == null) {
      return false;
    }
    final Matcher matcher = ETAG.matcher(tag);
    final boolean found = matcher.find();
    if (!found) {
      return false;
    }
    final String tagValue = matcher.group(1);
    final long decodedTime = Long.parseLong(tagValue, Character.MAX_RADIX);
    return (comparisonTime - decodedTime < (expiryPeriod * 1000))
        && (decodedTime >= updatedAt);
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
