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

package au.csiro.pathling.sql.dates.datetime;

import jakarta.annotation.Nonnull;
import javax.annotation.Nullable;
import org.hl7.fhir.r4.model.DateTimeType;

/**
 * Determines whether one datetime is before or at the same time as another.
 *
 * @author John Grimes
 */
public class DateTimeLessThanOrEqualToFunction extends DateTimeComparisonFunction {

  private static final long serialVersionUID = 787654631927909813L;

  public static final String FUNCTION_NAME = "datetime_lte";

  @Override
  @Nullable
  protected Boolean compare(@Nonnull final DateTimeType left, @Nonnull final DateTimeType right) {
    if (left.before(right)) {
      return true;
    }
    return left.equalsUsingFhirPathRules(right);
  }

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

}
