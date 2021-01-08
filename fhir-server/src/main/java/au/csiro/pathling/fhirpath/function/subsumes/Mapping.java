/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function.subsumes;

import au.csiro.pathling.fhirpath.encoding.SimpleCoding;
import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NonNull;

/**
 * Used to represent the results of $translate and $closure operations.
 *
 * @author John Grimes
 * @author Pior Szul
 */

@Data
@AllArgsConstructor(staticName = "of")
public class Mapping implements Serializable {

  private static final long serialVersionUID = 1L;

  @NonNull
  private final SimpleCoding from;

  @NonNull
  private final SimpleCoding to;
}
