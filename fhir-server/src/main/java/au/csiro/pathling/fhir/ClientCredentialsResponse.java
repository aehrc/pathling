/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhir;

import javax.annotation.Nullable;
import lombok.Data;

@Data
public class ClientCredentialsResponse {

  @Nullable
  private String accessToken;

  @Nullable
  private String tokenType;

  private int expiresIn;

  @Nullable
  private String refreshToken;

  @Nullable
  private String scope;

}
