package au.csiro.pathling.fhir;

import au.csiro.pathling.terminology.TerminologyService;
import java.io.Serializable;
import javax.annotation.Nonnull;
import org.slf4j.Logger;

public interface TerminologyClientFactory extends Serializable {

  /**
   * Builds a new instance.
   *
   * @param logger a {@link Logger} to use for logging
   * @return a shiny new TerminologyClient instance
   */
  @Nonnull
  TerminologyClient build(@Nonnull final Logger logger);

  /**
   * Builds a new instance.
   *
   * @param logger a {@link Logger} to use for logging
   * @return a shiny new TerminologyClient instance
   */
  @Nonnull
  TerminologyService buildService(@Nonnull final Logger logger);
}
