/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.async;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Used to annotate HAPI operations that are supported for asynchronous processing.
 *
 * @author John Grimes
 */
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
@Documented
public @interface AsyncSupported {

  /**
   * The asynchronous wire contract this operation follows. Selecting {@link
   * AsyncPattern#STANDARD_ASYNC_PATTERN} (the HL7 Asynchronous Interaction Request Pattern, <a
   * href="https://build.fhir.org/ig/HL7/api-incubator-ig/branches/simplified-async-interaction/async-interaction.html">spec</a>)
   * makes a completed job return 303 See Other with a redirect to the result endpoint, rather than
   * returning the result inline. Defaults to {@link AsyncPattern#BULK_DATA}.
   *
   * @return the asynchronous pattern for this operation
   */
  AsyncPattern pattern() default AsyncPattern.BULK_DATA;
}
