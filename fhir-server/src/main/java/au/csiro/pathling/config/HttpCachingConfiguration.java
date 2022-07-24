/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.config;

import java.util.List;
import javax.validation.constraints.NotNull;
import lombok.Data;

@Data
public class HttpCachingConfiguration {

  /**
   * A list of values to return within the Vary header.
   */
  @NotNull
  private List<String> vary;

  /**
   * A list of values to return within the Cache-Control header, for cacheable responses.
   */
  @NotNull
  private List<String> cacheableControl;

  /**
   * A list of values to return within the Cache-Control header, for uncacheable responses.
   */
  @NotNull
  private List<String> uncacheableControl;

}
