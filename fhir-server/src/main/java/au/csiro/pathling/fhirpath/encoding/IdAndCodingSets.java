/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
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

@Data
@NoArgsConstructor
@AllArgsConstructor
public class IdAndCodingSets implements Serializable {

  private static final long serialVersionUID = 1L;

  @Nullable
  private String id;

  @Nullable
  private List<SimpleCoding> inputCodings;

  @Nullable
  private List<SimpleCoding> argCodings;


  @Nonnull
  public List<SimpleCoding> safeGetInputCodings() {
    return inputCodings != null ? getInputCodings() : Collections.emptyList();
  }

  @Nonnull
  public List<SimpleCoding> safeGetArgCodings() {
    return argCodings != null ? getArgCodings() : Collections.emptyList();
  }

}
