/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function.subsumes.encoding;

import au.csiro.pathling.fhir.SimpleCoding;
import java.io.Serializable;
import java.util.List;
import javax.annotation.Nullable;
import lombok.Data;


@Data
public class IdAndCodingSets implements Serializable {

  private static final long serialVersionUID = 1L;

  @Nullable
  private String id;

  @Nullable
  private List<SimpleCoding> inputCodings;

  @Nullable
  private List<SimpleCoding> argCodings;
}
