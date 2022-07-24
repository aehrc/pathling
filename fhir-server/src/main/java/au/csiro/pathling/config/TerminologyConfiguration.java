/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.config;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import lombok.Data;
import org.hibernate.validator.constraints.URL;

/**
 * Represents configuration specific to the terminology functions of the server.
 */
@Data
public class TerminologyConfiguration {

  /**
   * Enables the use of terminology functions.
   */
  @NotNull
  private boolean enabled;

  /**
   * The endpoint of a FHIR terminology service (R4) that the server can use to resolve terminology
   * queries.
   */
  @NotBlank
  @URL
  private String serverUrl;

  /**
   * The maximum period (in milliseconds) that the server should wait for incoming data from the
   * terminology service.
   */
  @NotNull
  @Min(0)
  private Integer socketTimeout;

  /**
   * Setting this option to {@code true} will enable additional logging of the details of requests
   * between the server and the terminology service.
   */
  @NotNull
  private boolean verboseLogging;

  @NotNull
  private TerminologyAuthConfiguration authentication;

}
