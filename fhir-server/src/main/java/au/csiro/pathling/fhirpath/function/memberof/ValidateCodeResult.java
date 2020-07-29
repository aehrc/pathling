/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function.memberof;

import java.io.Serializable;
import lombok.Data;

/**
 * Used to represent the result of a $validate-code operation. The hash is used for correlation
 * between the input concepts and results, without needing to serialize the entire concept.
 *
 * @author John Grimes
 */
@Data
public class ValidateCodeResult implements Serializable {

  private static final long serialVersionUID = 4452195634956143175L;

  private int hash;
  private boolean result;

  /**
   * @param hash The value used for correlating input concepts to results
   */
  public ValidateCodeResult(final int hash) {
    this.hash = hash;
  }

  /**
   * @param hash The value used for correlating input concepts to results
   * @param result The boolean result of the operation
   */
  public ValidateCodeResult(final int hash, final boolean result) {
    this.hash = hash;
    this.result = result;
  }

}
