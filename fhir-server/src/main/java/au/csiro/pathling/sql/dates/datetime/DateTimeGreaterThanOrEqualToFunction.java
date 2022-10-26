/*
 * Copyright 2022 Commonwealth Scientific and Industrial Research
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

import java.util.function.BiFunction;
import javax.annotation.Nonnull;
import org.hl7.fhir.r4.model.DateTimeType;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * Determines whether one datetime is after or at the same time as another.
 *
 * @author John Grimes
 */
@Component
@Profile("core | unit-test")
public class DateTimeGreaterThanOrEqualToFunction extends DateTimeComparisonFunction {

  private static final long serialVersionUID = -4680194983727139029L;

  public static final String FUNCTION_NAME = "datetime_gte";

  @Nonnull
  @Override
  protected BiFunction<DateTimeType, DateTimeType, Boolean> getOperationFunction() {
    return (left, right) -> left.after(right) || left.equalsUsingFhirPathRules(right);
  }

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

}
