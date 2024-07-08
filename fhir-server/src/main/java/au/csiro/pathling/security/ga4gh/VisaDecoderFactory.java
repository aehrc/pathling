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

package au.csiro.pathling.security.ga4gh;

import static au.csiro.pathling.utilities.Preconditions.checkArgument;

import au.csiro.pathling.config.ServerConfiguration;
import jakarta.annotation.Nonnull;
import javax.annotation.Nullable;
import org.springframework.context.annotation.Profile;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtDecoderFactory;
import org.springframework.stereotype.Component;

/**
 * A JWT decoder specifically for visa tokens within GA4GH passports. This decoder does not validate
 * audience, and validates issuer against a list of allowed issuers within configuration.
 *
 * @author John Grimes
 */
@Component
@Profile("server & ga4gh")
public class VisaDecoderFactory implements JwtDecoderFactory<ServerConfiguration> {

  @Nonnull
  private final VisaDecoderBuilder builder;

  /**
   * @param builder a builder that can create a {@link JwtDecoder}
   */
  public VisaDecoderFactory(@Nonnull final VisaDecoderBuilder builder) {
    this.builder = builder;
  }

  @Override
  public JwtDecoder createDecoder(@Nullable final ServerConfiguration configuration) {
    checkArgument(configuration != null, "configuration cannot be null");
    return builder.build(configuration);
  }

}
