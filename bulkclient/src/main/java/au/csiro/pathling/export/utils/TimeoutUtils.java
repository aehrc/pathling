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

@UtilityClass
public class TimeoutUtils {

  public static final Duration TIMEOUT_NOW = Duration.ZERO.plusNanos(1);

  @Nonnull
  public static Instant toTimeoutAt(@Nonnull final Duration timeoutAfter) {
    return timeoutAfter.isNegative() || timeoutAfter.isZero()
           ? Instant.MAX
           : Instant.now().plus(timeoutAfter);
  }

  @Nonnull
  public static Duration toTimeoutAfter(@Nonnull final Instant timeoutAt) {
    final Instant now = Instant.now();
    if (now.isAfter(timeoutAt)) {
      return TIMEOUT_NOW;
    } else {
      return timeoutAt.equals(Instant.MAX)
             ? Duration.ZERO
             : Duration.between(now, timeoutAt);
    }
  }

  public static boolean hasExpired(@Nonnull final Instant timeoutAt) {
    return Instant.now().isAfter(timeoutAt);
  }
}
