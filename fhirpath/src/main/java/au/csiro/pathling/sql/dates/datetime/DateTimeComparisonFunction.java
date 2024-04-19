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

import au.csiro.pathling.sql.dates.TemporalComparisonFunction;
import ca.uhn.fhir.model.api.TemporalPrecisionEnum;
import ca.uhn.fhir.parser.DataFormatException;
import jakarta.annotation.Nullable;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Date;
import org.hl7.fhir.r4.model.DateTimeType;

/**
 * Base class for functions that compare datetimes.
 *
 * @author John Grimes
 */
public abstract class DateTimeComparisonFunction extends
    TemporalComparisonFunction<Object, DateTimeType> {

  private static final long serialVersionUID = -2449192480093120211L;

  @Nullable
  @Override
  protected DateTimeType parseEncodedValue(final Object value) {
    try {
      if (value instanceof Timestamp) {
        final Instant instant = ((Timestamp) value).toInstant();
        return new DateTimeType(Date.from(instant), TemporalPrecisionEnum.MILLI);
      } else {
        return new DateTimeType((String) value);
      }
    } catch (final NullPointerException | IllegalArgumentException | DataFormatException e) {
      // If we are unable to parse the value, the result will be null.
      return null;
    }
  }

}
