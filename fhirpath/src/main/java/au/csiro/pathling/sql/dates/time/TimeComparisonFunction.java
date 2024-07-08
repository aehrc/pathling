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

import au.csiro.pathling.sql.dates.TemporalComparisonFunction;
import jakarta.annotation.Nullable;
import java.time.LocalTime;

/**
 * Base class for functions that compare times.
 *
 * @author John Grimes
 */
public abstract class TimeComparisonFunction extends TemporalComparisonFunction<String, LocalTime> {

  private static final long serialVersionUID = 3661335567427062952L;

  @Nullable
  @Override
  protected LocalTime parseEncodedValue(final String value) {
    return TimeFunction.parseEncodedTime(value);
  }

}
