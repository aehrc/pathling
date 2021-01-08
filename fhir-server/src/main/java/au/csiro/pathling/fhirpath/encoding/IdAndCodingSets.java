/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.encoding;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Represents a resource identity along with a set of input codings, and a set of argument codings.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class IdAndCodingSets implements Serializable {

  private static final long serialVersionUID = 1L;

  @Nullable
  private String id;

  @Nullable
  private List<Integer> eid;

  @Nullable
  private List<SimpleCoding> inputCodings;

  @Nullable
  private List<SimpleCoding> argCodings;


  /**
   * @return a list of {@link SimpleCoding} objects, or an empty list
   */
  @Nonnull
  public List<SimpleCoding> safeGetInputCodings() {
    return getInputCodings() != null
           ? getInputCodings()
           : Collections.emptyList();
  }

  /**
   * @return a list of {@link SimpleCoding} objects, or an empty list
   */
  @Nonnull
  public List<SimpleCoding> safeGetArgCodings() {
    return getArgCodings() != null
           ? getArgCodings()
           : Collections.emptyList();
  }

}
