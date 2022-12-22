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

package au.csiro.pathling.config;

import au.csiro.pathling.config.HttpClientCachingConfiguration.ValidHttpCacheConfiguration;
import java.io.Serializable;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.validation.Constraint;
import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import javax.validation.Payload;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.http.impl.client.cache.CacheConfig;

/**
 * Represents configuration relating to HTTP client caching.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@ValidHttpCacheConfiguration
public class HttpClientCachingConfiguration implements Serializable {

  private static final long serialVersionUID = -3030386957343963899L;

  public static final boolean DEFAULT_ENABLED = true;
  public static final int DEFAULT_MAX_CACHE_ENTRIES = 100_000;
  public static final long DEFAULT_MAX_OBJECT_SIZE = 64_000L;
  public static final StorageType DEFAULT_STORAGE_TYPE = StorageType.MEMORY;
  public static final int DEFAULT_EXPIRY = 600;

  /**
   * Enables client side caching of REST requests.
   */
  @NotNull
  @Builder.Default
  private boolean enabled = DEFAULT_ENABLED;

  /**
   * Sets the maximum number of entries the cache will retain.
   * <p>
   * See also: {@link CacheConfig.Builder#setMaxCacheEntries(int)}
   */
  @NotNull
  @Min(0)
  @Builder.Default
  private int maxEntries = DEFAULT_MAX_CACHE_ENTRIES;

  /**
   * Sets the maximum size of a cacheable response, in bytes.
   *
   * @see CacheConfig.Builder#setMaxObjectSize(long)
   */
  @Min(0)
  @NotNull
  @Builder.Default
  private long maxObjectSize = DEFAULT_MAX_OBJECT_SIZE;

  /**
   * The type of storage to use for the cache.
   *
   * @see StorageType
   */
  @NotNull
  @Builder.Default
  private StorageType storageType = DEFAULT_STORAGE_TYPE;

  /**
   * The path on disk to use for the cache, required when {@link StorageType#DISK} is specified.
   */
  @Nullable
  private String storagePath;

  /**
   * The default expiry time for cache entries (in seconds), used when the server does not provide
   * an expiry value.
   */
  @Min(0)
  @NotNull
  @Builder.Default
  private int defaultExpiry = DEFAULT_EXPIRY;

  /**
   * Represents the type of storage used by the cache.
   */
  public enum StorageType {
    /**
     * The cache is stored in memory, and is reset when the application is restarted.
     */
    MEMORY("memory"),

    /**
     * The cache is stored on disk, and is persisted between application restarts.
     */
    DISK("disk");

    @Nonnull
    private final String code;

    // NOTE: Not using @Nonnull annotation here on purpose to avoid warning:
    // "Constraints on the parameters of constructors of non-static inner classes are not supported 
    // if those parameters have a generic type due to JDK bug JDK-5087240."
    // and a lengthy stack trace.
    StorageType(final String code) {
      this.code = Objects.requireNonNull(code);
    }

    @Override
    public String toString() {
      return code;
    }

    @Nonnull
    public static StorageType fromCode(@Nonnull final String code) {
      for (final StorageType storageType : values()) {
        if (storageType.code.equals(code)) {
          return storageType;
        }
      }
      throw new IllegalArgumentException("Unknown storage type: " + code);
    }
  }

  public static HttpClientCachingConfiguration defaults() {
    return HttpClientCachingConfiguration.builder().build();
  }

  public static HttpClientCachingConfiguration disabled() {
    return HttpClientCachingConfiguration.builder().enabled(false).build();
  }

  @Target({ElementType.TYPE, ElementType.ANNOTATION_TYPE})
  @Retention(RetentionPolicy.RUNTIME)
  @Constraint(validatedBy = HttpCacheConfigurationValidator.class)
  @Documented
  public @interface ValidHttpCacheConfiguration {

    String message() default "If the storage type is disk, then a storage path must be supplied.";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};

  }

  public static class HttpCacheConfigurationValidator implements
      ConstraintValidator<ValidHttpCacheConfiguration, HttpClientCachingConfiguration> {

    @Override
    public void initialize(final ValidHttpCacheConfiguration constraintAnnotation) {
    }

    @Override
    public boolean isValid(final HttpClientCachingConfiguration value,
        final ConstraintValidatorContext context) {
      if (value.getStorageType().equals(StorageType.DISK)) {
        return value.getStoragePath() != null;
      } else {
        return true;
      }
    }
  }

}
