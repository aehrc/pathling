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

import au.csiro.pathling.async.Job.JobTag;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Value;

/**
 * Represents a tag which includes all aspects a request that need to be compared in order to
 * determine if they represent identical requests. Two identical request should produce identical
 * results.
 */
@Value
public class RequestTag implements JobTag {

  @Nonnull String requestUrl;

  @Nonnull Map<String, List<String>> varyHeaders;

  @Nonnull Optional<String> cacheKey;

  /**
   * Operation-specific cache key component, computed from request body parameters. Empty for
   * operations that don't override {@link PreAsyncValidation#computeCacheKeyComponent}.
   */
  @Nonnull String operationCacheKey;

  // toString() is overridden to exclude the varyHeaders field, which can contain sensitive data.
  @Override
  public String toString() {
    return "RequestTag("
        + "requestUrl='"
        + requestUrl
        + "'"
        + ", varyHeaders=List@"
        + Integer.toHexString(varyHeaders.hashCode())
        + ", cacheKey="
        + cacheKey
        + ", operationCacheKey='"
        + operationCacheKey
        + "'"
        + ')';
  }
}
