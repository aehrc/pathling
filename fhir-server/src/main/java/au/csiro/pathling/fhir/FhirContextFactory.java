/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.csiro.pathling.fhir;

import au.csiro.pathling.encoders.FhirEncoders;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import jakarta.annotation.Nonnull;
import java.io.Serializable;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * Uses the FhirEncoders class to create a FhirContext. Used for code that runs on Spark workers.
 *
 * @author John Grimes
 */
@Component
@Profile({"core", "fhir"})
public class FhirContextFactory implements Serializable {

  private static final long serialVersionUID = 3704272891614244206L;

  @Nonnull
  private final FhirVersionEnum fhirVersion;

  /**
   * @param fhirContext the {@link FhirContext} that this factory should be based upon
   */
  public FhirContextFactory(@Nonnull final FhirContext fhirContext) {
    this.fhirVersion = fhirContext.getVersion().getVersion();
  }

  /**
   * @return a new {@link FhirContext} instance
   */
  @Nonnull
  public FhirContext build() {
    return FhirEncoders.contextFor(fhirVersion);
  }

}
