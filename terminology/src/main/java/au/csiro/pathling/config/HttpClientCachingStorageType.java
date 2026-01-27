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

package au.csiro.pathling.config;

import static java.util.Objects.requireNonNull;

import jakarta.annotation.Nonnull;

/**
 * Represents the type of storage used by the cache.
 *
 * @author John Grimes
 */
public enum HttpClientCachingStorageType {
  /** The cache is stored in memory, and is reset when the application is restarted. */
  MEMORY("memory"),

  /** The cache is stored on disk, and is persisted between application restarts. */
  DISK("disk");

  @Nonnull private final String code;

  HttpClientCachingStorageType(@Nonnull final String code) {
    this.code = requireNonNull(code);
  }

  @Override
  public String toString() {
    return code;
  }

  /**
   * Returns the storage type corresponding to the given code.
   *
   * @param code the code of the storage type
   * @return the HttpClientCachingStorageType corresponding to the given code
   */
  @Nonnull
  public static HttpClientCachingStorageType fromCode(@Nonnull final String code) {
    for (final HttpClientCachingStorageType storageType : values()) {
      if (storageType.code.equals(code)) {
        return storageType;
      }
    }
    throw new IllegalArgumentException("Unknown storage type: " + code);
  }
}
