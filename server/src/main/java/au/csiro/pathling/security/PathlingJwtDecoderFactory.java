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

package au.csiro.pathling.security;

import static au.csiro.pathling.utilities.Preconditions.checkArgument;

import au.csiro.pathling.config.ServerConfiguration;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtDecoderFactory;
import org.springframework.stereotype.Component;

/**
 * A JWT decoder that is capable of using the issuer claim within the token to retrieve the JWKS.
 *
 * @author John Grimes
 */
@Component
@ConditionalOnProperty(prefix = "pathling", name = "auth.enabled", havingValue = "true")
public class PathlingJwtDecoderFactory implements JwtDecoderFactory<ServerConfiguration> {

  @Nonnull private final PathlingJwtDecoderBuilder builder;

  /**
   * Creates a new PathlingJwtDecoderFactory.
   *
   * @param builder a builder that can create a {@link JwtDecoder}
   */
  public PathlingJwtDecoderFactory(@Nonnull final PathlingJwtDecoderBuilder builder) {
    this.builder = builder;
  }

  @Override
  public JwtDecoder createDecoder(@Nullable final ServerConfiguration configuration) {
    checkArgument(configuration != null, "configuration cannot be null");
    return builder.build(configuration);
  }

  /**
   * Creates a JwtDecoder bean for Spring Security.
   *
   * @param configuration that controls the behaviour of the decoder factory
   * @param factory a factory that can create a {@link JwtDecoder}
   * @return a shiny new {@link JwtDecoder}
   */
  @Bean
  public static JwtDecoder pathlingJwtDecoder(
      @Nullable final ServerConfiguration configuration,
      @Nonnull final PathlingJwtDecoderFactory factory) {
    return factory.createDecoder(configuration);
  }
}
