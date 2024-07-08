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

package au.csiro.pathling.terminology;

import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.utilities.ResourceCloser;
import ca.uhn.fhir.rest.server.exceptions.BaseServerResponseException;
import jakarta.annotation.Nonnull;
import java.io.Closeable;
import javax.annotation.Nullable;

/**
 * Common functionality for all implementations of {@link TerminologyService}.
 *
 * @author John Grimes
 */
public abstract class BaseTerminologyService extends ResourceCloser implements TerminologyService {

  @Nonnull
  protected final TerminologyClient terminologyClient;

  public BaseTerminologyService(@Nonnull final TerminologyClient terminologyClient,
      @Nonnull final Closeable... resourcesToClose) {
    super(resourcesToClose);
    this.terminologyClient = terminologyClient;
  }

  /**
   * This method allows us to be tolerant to invalid inputs to terminology operations, which produce
   * 400-series errors from the terminology server. A result can be provided which will be returned
   * in the event of such an error.
   *
   * @param <ResultType> the type of the result
   * @param e the exception that was thrown
   * @param invalidInputReturnValue the value to return in the event of an invalid input
   * @return the result, which may be the fallback
   */
  @Nullable
  public static <ResultType> ResultType handleError(@Nonnull final BaseServerResponseException e,
      @Nullable final ResultType invalidInputReturnValue) {
    if (e.getStatusCode() / 100 == 4) {
      return invalidInputReturnValue;
    } else {
      throw e;
    }
  }
}
