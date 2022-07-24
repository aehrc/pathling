/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.config;

import java.util.List;
import javax.validation.constraints.NotNull;
import lombok.Data;

/**
 * Represents configuration specific to import functionality.
 */
@Data
public class ImportConfiguration {

  /**
   * A set of URL prefixes which are allowable for use within the import operation.
   */
  @NotNull
  private List<String> allowableSources;

}
