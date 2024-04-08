/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.csiro.pathling.export.utils;

import lombok.experimental.UtilityClass;
import javax.annotation.Nonnull;
import java.time.Duration;
import java.time.Instant;

/**
 * Utility methods for working with timeouts.
 */
@UtilityClass
public class TimeoutUtils {

  /**
   * Represents a timeout that expires immediately. This is different thatn a zero duration timeout
   * which represents an infinite timeout.
   */
  public static final Duration TIMEOUT_NOW = Duration.ZERO.plusNanos(1);

  /**
   * Represents an infinite timeout.
   */
  public static final Duration TIMEOUT_INFINITE = Duration.ZERO;


  /**
   * Checks if the given timeout is infinite.
   * 
   * @param timeout the timeout to check.
   * @return true if the timeout is infinite, false otherwise.
   */
  public static boolean isInfinite(@Nonnull final Duration timeout) {
    return timeout.isZero() || timeout.isNegative();
  }

  /**
   * Computes an instant representing the time at which the given timeout will expire.
   *
   * @param timeoutAfter the duration after which the timeout will expire. If the duration is zero
   * or negative, the timeout will expire immediately.
   * @return the instant at which the timeout will expire.
   */
  @Nonnull
  public static Instant toTimeoutAt(@Nonnull final Duration timeoutAfter) {
    return isInfinite(timeoutAfter)
           ? Instant.MAX
           : Instant.now().plus(timeoutAfter);
  }

  /**
   * Computes a duration representing the time remaining until the given timeout expires. If the
   * timeout has already expired, the duration will TIMEOUT_NOW.
   *
   * @param timeoutAt the instant at which the timeout should expire.
   * @return the duration remaining until the timeout expires.
   */
  @Nonnull
  public static Duration toTimeoutAfter(@Nonnull final Instant timeoutAt) {
    final Instant now = Instant.now();
    if (now.isAfter(timeoutAt)) {
      return TIMEOUT_NOW;
    } else {
      return timeoutAt.equals(Instant.MAX)
             ? TIMEOUT_INFINITE
             : Duration.between(now, timeoutAt);
    }
  }

  /**
   * Checks if the given timeout has expired.
   *
   * @param timeoutAt the instant at which the timeout should expire.
   * @return true if the timeout has expired, false otherwise.
   */
  public static boolean hasExpired(@Nonnull final Instant timeoutAt) {
    return Instant.now().isAfter(timeoutAt);
  }
}
