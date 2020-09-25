/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function.subsumes.encoding;

import java.io.Serializable;
import javax.annotation.Nullable;
import lombok.Data;

/**
 * Used for representing results of functions that return boolean values.
 *
 * @author John Grimes
 */
@Data
public class IdAndBoolean implements Serializable {

  private static final long serialVersionUID = 1L;

  @Nullable
  private String id;

  @Nullable
  private Boolean value;

  public IdAndBoolean(String id, Boolean value) {
    this.id = id;
    this.value = value;
  }

  public static IdAndBoolean of(String id, Boolean value) {
    return new IdAndBoolean(id, value);
  }

  @Override
  public String toString() {
    return "IdAndBoolean{" +
        "id='" + id + '\'' +
        ", value=" + value +
        '}';
  }
}
