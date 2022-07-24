/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.config;

import java.util.Set;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import lombok.Data;

/**
 * Represents configuration specific to FHIR encoding.
 */
@Data
public class EncodingConfiguration {

  /**
   * The maximum nesting level for recursive data types.
   */
  @NotNull
  @Min(0)
  private Integer maxNestingLevel;

  /**
   * The list of types that are encoded within open types, such as extensions.
   */
  @NotNull
  private Set<String> openTypes;

  /**
   * Enables support for FHIR extensions.
   */
  @NotNull
  private boolean enableExtensions;

}
