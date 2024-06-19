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

import au.csiro.pathling.sql.udf.AbstractUDFRegistrar;

/**
 * Registration of the datetime UDFs.
 */
public class DateTimeUDFRegistrar extends AbstractUDFRegistrar {

  @Override
  protected void registerUDFs(final UDFRegistrar udfRegistrar) {
    udfRegistrar
        .register(new DateTimeAddDurationFunction())
        .register(new DateTimeSubtractDurationFunction())
        .register(new DateTimeEqualsFunction())
        .register(new DateTimeGreaterThanFunction())
        .register(new DateTimeGreaterThanOrEqualToFunction())
        .register(new DateTimeLessThanFunction())
        .register(new DateTimeLessThanOrEqualToFunction())
        .register(new NormalizeDateTimeFunction());
  }
}
