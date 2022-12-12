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

package au.csiro.pathling.terminology;

import ca.uhn.fhir.rest.gclient.IOperationUntypedWithInput;
import java.util.Optional;
import javax.annotation.Nonnull;


/**
 * An abstraction of a terminology operation that describes how to validate, execute and extract a
 * result from the response.
 *
 * @param <ResponseType> The type of the response returned by the terminology client
 * @param <ResultType> The type of the final result that is extracted from the response
 * @author John Grimes
 */
public interface TerminologyOperation<ResponseType, ResultType> {

  @Nonnull
  Optional<ResultType> validate();

  @Nonnull
  IOperationUntypedWithInput<ResponseType> buildRequest();

  @Nonnull
  ResultType extractResult(@Nonnull final ResponseType response);

  @Nonnull
  ResultType invalidRequestFallback();

}
