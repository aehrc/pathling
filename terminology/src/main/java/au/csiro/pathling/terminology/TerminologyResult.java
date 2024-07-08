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

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.Serializable;
import lombok.Value;

/**
 * Represents the result of a terminology operation, along with metadata that can be used for
 * caching.
 *
 * @param <ResultType> The type of the final result that is extracted from the response
 * @author John Grimes
 */
@Value
public class TerminologyResult<ResultType extends Serializable> implements Serializable {

  private static final long serialVersionUID = -8569345179387329139L;

  /**
   * The value of the cache entry.
   */
  @Nullable
  ResultType data;

  /**
   * The ETag returned in the response that generated this value.
   */
  @Nullable
  String eTag;

  /**
   * The expiry time in milliseconds since the epoch.
   */
  @Nullable
  Long expires;

  /**
   * Set to true if the response was a 304 Not Modified.
   */
  @Nonnull
  Boolean notModified;

}
