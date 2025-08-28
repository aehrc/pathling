/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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

import au.csiro.pathling.config.TerminologyAuthConfiguration.ValidTerminologyAuthConfiguration;
import jakarta.annotation.Nullable;
import jakarta.validation.Constraint;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import jakarta.validation.Payload;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import java.io.Serial;
import java.io.Serializable;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import lombok.Builder;
import lombok.Data;
import lombok.ToString;
import org.hibernate.validator.constraints.URL;

/**
 * Configuration relating to authentication of requests to the terminology service.
 *
 * @author John Grimes
 */
@Data
@Builder
@ValidTerminologyAuthConfiguration
public class TerminologyAuthConfiguration implements Serializable {

  @Serial
  private static final long serialVersionUID = 6321330066417583745L;

  /**
   * Enables authentication of requests to the terminology server.
   */
  @NotNull
  @Builder.Default
  private boolean enabled = false;

  /**
   * An OAuth2 token endpoint for use with the client credentials grant.
   */
  @Nullable
  @URL
  private String tokenEndpoint;

  /**
   * A client ID for use with the client credentials grant.
   */
  @Nullable
  private String clientId;

  /**
   * A client secret for use with the client credentials grant.
   */
  @Nullable
  @ToString.Exclude
  private String clientSecret;

  /**
   * A scope value for use with the client credentials grant.
   */
  @Nullable
  private String scope;

  /**
   * The minimum number of seconds that a token should have before expiry when deciding whether to
   * send it with a terminology request.
   */
  @NotNull
  @Min(0)
  @Builder.Default
  private long tokenExpiryTolerance = 120;

  /**
   * Validation annotation for terminology authentication configuration.
   */
  @Target({ElementType.TYPE, ElementType.ANNOTATION_TYPE})
  @Retention(RetentionPolicy.RUNTIME)
  @Constraint(validatedBy = TerminologyAuthConfigValidator.class)
  @Documented
  public @interface ValidTerminologyAuthConfiguration {

    /**
     * The validation error message.
     *
     * @return the error message
     */
    String message() default "If terminology authentication is enabled, token endpoint, "
        + "client ID and client secret must be supplied.";

    /**
     * The validation groups.
     *
     * @return the validation groups
     */
    Class<?>[] groups() default {};

    /**
     * The validation payload.
     *
     * @return the validation payload
     */
    Class<? extends Payload>[] payload() default {};

  }

  /**
   * Validator for terminology authentication configuration.
   */
  public static class TerminologyAuthConfigValidator implements
      ConstraintValidator<ValidTerminologyAuthConfiguration, TerminologyAuthConfiguration> {

    @Override
    public boolean isValid(final TerminologyAuthConfiguration value,
        final ConstraintValidatorContext context) {
      if (value.isEnabled()) {
        return value.getTokenEndpoint() != null && value.getClientId() != null
            && value.getClientSecret() != null;
      }
      return true;
    }

  }

}
