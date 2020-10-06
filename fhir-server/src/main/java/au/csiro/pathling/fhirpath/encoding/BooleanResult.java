/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.encoding;

import java.io.Serializable;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Used for representing results of functions that return boolean values.
 *
 * @author John Grimes
 */
@Data
@AllArgsConstructor(staticName = "of")
public class BooleanResult implements Serializable {

  private static final long serialVersionUID = 1L;

  @Nullable
  private String id;

  @Nullable
  private Boolean value;

  /**
   * Creates a NULL boolean result for given id.
   *
   * @param id the ID to use within the result
   * @return NULL boolean result
   */
  @Nonnull
  public static BooleanResult nullOf(@Nullable final String id) {
    return new BooleanResult(id, null);
  }
}
