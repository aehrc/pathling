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

package au.csiro.pathling.export.ws;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.Value;
import org.apache.http.client.utils.DateUtils;

/**
 * Represents a value for the Retry-After header. Either the number of seconds to wait, or an HTTP
 * date to wait until.
 */
public interface RetryValue {

  /**
   * Represents a value for the Retry-After header that is a specific date.
   */
  @Value
  class At implements RetryValue {

    /**
     * The date to wait until.
     */
    @Nonnull
    Instant value;

    @Nonnull
    @Override
    public Duration until(@Nonnull final Instant time) {
      return Duration.between(time, value);
    }

    @Override
    @Nonnull
    public String toString() {
      return "at(" + value + ")";
    }
  }

  /**
   * Represents a value for the Retry-After header that is a number of seconds.
   */
  @Value
  class After implements RetryValue {

    // The duration to wait
    @Nonnull
    Duration value;

    @Nonnull
    @Override
    public Duration until(@Nonnull final Instant time) {
      return value;
    }

    @Override
    @Nonnull
    public String toString() {
      return "after(" + value + ")";
    }
  }

  /**
   * Creates a RetryValue that represents a duration to wait.
   *
   * @param after the duration to wait
   * @return a RetryValue
   */
  @Nonnull
  static RetryValue after(@Nonnull final Duration after) {
    return new After(after);
  }

  /**
   * Creates a RetryValue that represents a specific date to wait until.
   *
   * @param at the date to wait until
   * @return a RetryValue
   */
  @Nonnull
  static RetryValue at(@Nonnull final Instant at) {
    return new At(at);
  }

  /**
   * Returns the duration this value represents until the given time.
   *
   * @param time the time to wait until
   * @return the duration until the given time
   */
  @Nonnull
  Duration until(@Nonnull final Instant time);

  /**
   * Parses a Retry-After header value that is either a number of seconds or an HTTP date.
   *
   * @param retry the value to parse
   * @return a RetryValue if the value could be parsed, or empty if it could not
   */
  @Nonnull
  static Optional<RetryValue> parseHttpValue(@Nonnull final String retry) {
    try {
      final int seconds = Integer.parseInt(retry);
      return seconds > 0
             ? Optional.of(after(Duration.ofSeconds(seconds)))
             : Optional.empty();
    } catch (final NumberFormatException __) {
      // ignore
    }
    // Try to parse as HTTP date
    return Optional.ofNullable(DateUtils.parseDate(retry)).map(d -> at(d.toInstant()));
  }
}
