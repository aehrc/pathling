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

import java.util.function.BiFunction;
import org.hl7.fhir.r4.model.DateTimeType;
import org.hl7.fhir.r4.model.Quantity;

/**
 * Adds a duration to a datetime.
 *
 * @author John Grimes
 */
public class DateTimeAddDurationFunction extends DateTimeArithmeticFunction {

  private static final long serialVersionUID = 6922227603585641053L;

  public static final String FUNCTION_NAME = "datetime_add_duration";

  @Override
  protected BiFunction<DateTimeType, Quantity, DateTimeType> getOperationFunction() {
    return this::performAddition;
  }

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

}
