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

package au.csiro.pathling.sql.dates.time;

import jakarta.annotation.Nonnull;
import java.time.LocalTime;
import javax.annotation.Nullable;

/**
 * Determines whether one time is after another.
 *
 * @author John Grimes
 */
public class TimeGreaterThanFunction extends TimeComparisonFunction {

  private static final long serialVersionUID = 1785863110270729355L;

  public static final String FUNCTION_NAME = "time_gt";

  @Override
  @Nullable
  protected Boolean compare(@Nonnull final LocalTime left, @Nonnull final LocalTime right) {
    return left.isAfter(right);
  }

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

}
