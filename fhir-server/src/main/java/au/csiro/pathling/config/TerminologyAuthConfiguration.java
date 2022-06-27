/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.config;

import au.csiro.pathling.config.TerminologyAuthConfiguration.ValidTerminologyAuthConfiguration;
import java.io.Serializable;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import javax.annotation.Nullable;
import javax.validation.Constraint;
import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import javax.validation.Payload;
import javax.validation.constraints.NotNull;
import lombok.Data;
import lombok.ToString;
import org.hibernate.validator.constraints.URL;

@Data
@ValidTerminologyAuthConfiguration
public class TerminologyAuthConfiguration implements Serializable {

  private static final long serialVersionUID = 6321330066417583745L;

  /**
   * Enables authentication of requests to the terminology server.
   */
  @NotNull
  private boolean enabled;

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

  @Target({ElementType.TYPE, ElementType.ANNOTATION_TYPE})
  @Retention(RetentionPolicy.RUNTIME)
  @Constraint(validatedBy = TerminologyAuthConfigValidator.class)
  @Documented
  public static @interface ValidTerminologyAuthConfiguration {

    String message() default "If terminology authentication is enabled, token endpoint, "
        + "client ID and client secret must be supplied.";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};

  }

  public static class TerminologyAuthConfigValidator implements
      ConstraintValidator<ValidTerminologyAuthConfiguration, TerminologyAuthConfiguration> {

    @Override
    public void initialize(final ValidTerminologyAuthConfiguration constraintAnnotation) {
    }

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
