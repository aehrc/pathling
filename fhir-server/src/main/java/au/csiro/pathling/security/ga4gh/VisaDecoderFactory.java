/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.security.ga4gh;

import static au.csiro.pathling.utilities.Preconditions.checkArgument;

import au.csiro.pathling.config.Configuration;
import javax.annotation.Nonnull;
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
public class VisaDecoderFactory implements JwtDecoderFactory<Configuration> {

  @Nonnull
  private final VisaDecoderBuilder builder;

  /**
   * @param builder a builder that can create a {@link JwtDecoder}
   */
  public VisaDecoderFactory(@Nonnull final VisaDecoderBuilder builder) {
    this.builder = builder;
  }

  @Override
  public JwtDecoder createDecoder(@Nullable final Configuration configuration) {
    checkArgument(configuration != null, "configuration cannot be null");
    return builder.build(configuration);
  }

}
