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

package au.csiro.pathling.config;

import static java.util.Objects.nonNull;

import au.csiro.pathling.config.HttpClientCachingConfiguration.ValidHttpCacheConfiguration;
import jakarta.annotation.Nullable;
import java.io.Serializable;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import javax.validation.Constraint;
import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import javax.validation.Payload;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import lombok.Builder;
import lombok.Data;

/**
 * Configuration relating to the caching of terminology requests.
 *
 * @author John Grimes
 * @author Piotr Szul
 */
@Data
@Builder
@ValidHttpCacheConfiguration
public class HttpClientCachingConfiguration implements Serializable {

  private static final long serialVersionUID = -3030386957343963899L;

  /**
   * Set this to false to disable caching of terminology requests (not recommended).
   */
  @NotNull
  @Builder.Default
  private boolean enabled = true;

  /**
   * Sets the maximum number of entries that will be held in memory.
   */
  @NotNull
  @Min(0)
  @Builder.Default
  private int maxEntries = 200_000;

  /**
   * The {@link HttpClientCachingStorageType} to use for the cache.
   */
  @NotNull
  @Builder.Default
  private HttpClientCachingStorageType storageType = HttpClientCachingStorageType.MEMORY;

  /**
   * The path on disk to use for the cache, required when {@link HttpClientCachingStorageType#DISK}
   * is specified.
   */
  @Nullable
  private String storagePath;

  /**
   * The default expiry time for cache entries (in seconds), used when the server does not provide
   * an expiry value.
   */
  @Min(0)
  @Builder.Default
  private int defaultExpiry = 600;

  /**
   * If provided, this value overrides the expiry time provided by the terminology server.
   */
  @Nullable
  @Min(0)
  private Integer overrideExpiry;

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
      if (HttpClientCachingStorageType.DISK.equals(value.getStorageType())) {
        return nonNull(value.getStoragePath());
      } else {
        return true;
      }
    }
  }

}
