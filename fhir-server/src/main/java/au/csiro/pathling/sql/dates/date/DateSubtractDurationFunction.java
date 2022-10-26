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

package au.csiro.pathling.sql.dates.date;

import java.util.function.BiFunction;
import org.hl7.fhir.r4.model.DateType;
import org.hl7.fhir.r4.model.Quantity;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * Subtracts a duration from a date.
 *
 * @author John Grimes
 */
@Component
@Profile("core | unit-test")
public class DateSubtractDurationFunction extends DateArithmeticFunction {

  private static final long serialVersionUID = 5201879133976866457L;

  public static final String FUNCTION_NAME = "date_subtract_duration";

  @Override
  protected BiFunction<DateType, Quantity, DateType> getOperationFunction() {
    return this::performSubtraction;
  }

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

}
