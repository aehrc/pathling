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

import lombok.Data;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.io.Serializable;

@Data
// TODO: Add validation is needed
//@ValidTerminologyAuthConfiguration
public class HttpClientConfiguration implements Serializable {

  private static final long serialVersionUID = -1624276800166930462L;
  /**
   * The maximum total number of connections for the client. Also see: {@link
   * org.apache.http.impl.client.HttpClientBuilder#setMaxConnTotal(int)}
   */
  @Min(0)
  private int maxConnectionsTotal;

  /**
   * The maximum number of connections per route for the client. Also see: {@link
   * org.apache.http.impl.client.HttpClientBuilder#setMaxConnPerRoute(int)}
   */
  @Min(0)
  private int maxConnectionsPerRoute;

  @NotNull
  private HttpCacheConfiguration cache;

  // @Target({ElementType.TYPE, ElementType.ANNOTATION_TYPE})
  // @Retention(RetentionPolicy.RUNTIME)
  // @Constraint(validatedBy = TerminologyAuthConfigValidator.class)
  // @Documented
  // public static @interface ValidTerminologyAuthConfiguration {
  //
  //   String message() default "If terminology authentication is enabled, token endpoint, "
  //       + "client ID and client secret must be supplied.";
  //
  //   Class<?>[] groups() default {};
  //
  //   Class<? extends Payload>[] payload() default {};
  //
  // }
  //
  // public static class TerminologyAuthConfigValidator implements
  //     ConstraintValidator<ValidTerminologyAuthConfiguration, TerminologyClientConfiguration> {
  //
  //   @Override
  //   public void initialize(final ValidTerminologyAuthConfiguration constraintAnnotation) {
  //   }
  //
  //   @Override
  //   public boolean isValid(final TerminologyClientConfiguration value,
  //       final ConstraintValidatorContext context) {
  //     if (value.isEnabled()) {
  //       return value.getTokenEndpoint() != null && value.getClientId() != null
  //           && value.getClientSecret() != null;
  //     }
  //     return true;
  //   }
  //
  // }

}
