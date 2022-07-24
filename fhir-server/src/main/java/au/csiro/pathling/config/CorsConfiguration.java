/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.config;

import java.util.List;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import lombok.Data;

/**
 * Represents configuration relating to Cross-Origin Resource Sharing (CORS).
 */
@Data
public class CorsConfiguration {

  @NotNull
  private List<String> allowedOrigins;

  @NotNull
  private List<String> allowedOriginPatterns;

  @NotNull
  private List<String> allowedMethods;

  @NotNull
  private List<String> allowedHeaders;

  @NotNull
  private List<String> exposedHeaders;

  @NotNull
  @Min(0)
  private Long maxAge;

}
