/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhir;

import javax.annotation.Nullable;
import lombok.Value;

@Value
public class ClientCredentialsResponse {

  @Nullable
  String accessToken;

  @Nullable
  String tokenType;

  int expiresIn;

  @Nullable
  String refreshToken;

  @Nullable
  String scope;

}
