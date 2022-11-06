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

import au.csiro.pathling.sql.dates.TemporalArithmeticFunction;
import java.util.function.Function;
import org.hl7.fhir.r4.model.DateType;

/**
 * Base class for functions that perform arithmetic on dates.
 *
 * @author John Grimes
 */
public abstract class DateArithmeticFunction extends TemporalArithmeticFunction<String, DateType> {

  private static final long serialVersionUID = 6759548804191034570L;

  @Override
  protected Function<String, DateType> parseEncodedValue() {
    return DateType::new;
  }

  @Override
  protected Function<DateType, String> encodeResult() {
    return DateType::getValueAsString;
  }

}
