/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.extract;

import java.util.Optional;
import lombok.Value;

@Value
public class Result {

  String url;
  Optional<String> ownerId;

}
