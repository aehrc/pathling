/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.terminology.validatecode;

import au.csiro.pathling.fhirpath.encoding.ImmutableCoding;
import au.csiro.pathling.terminology.TerminologyParameters;
import jakarta.annotation.Nonnull;
import org.hl7.fhir.r4.model.Coding;

/**
 * Represents the input parameters to the validate-code operation.
 *
 * @param valueSetUrl the URL of the value set to validate against
 * @param coding the coding to validate
 * @param inferSystem whether the server should infer the system of the code from the value set
 * @author John Grimes
 * @see <a
 *     href="https://www.hl7.org/fhir/R4/valueset-operation-validate-code.html">ValueSet/$validate-code</a>
 */
public record ValidateCodeParameters(
    @Nonnull String valueSetUrl, @Nonnull ImmutableCoding coding, boolean inferSystem)
    implements TerminologyParameters {

  /**
   * Creates parameters to validate a coding, using the system of the coding.
   *
   * @param valueSetUrl the URL of the value set to validate against
   * @param coding the coding to validate
   */
  public ValidateCodeParameters(
      @Nonnull final String valueSetUrl, @Nonnull final ImmutableCoding coding) {
    this(valueSetUrl, coding, false);
  }

  /**
   * Creates parameters to validate a code without a system, inferring the system from the value
   * set.
   *
   * @param valueSetUrl the URL of the value set to validate against
   * @param code the code to validate
   * @return the parameters
   */
  @Nonnull
  public static ValidateCodeParameters inferringSystem(
      @Nonnull final String valueSetUrl, @Nonnull final String code) {
    return new ValidateCodeParameters(
        valueSetUrl, ImmutableCoding.of(new Coding().setCode(code)), true);
  }
}
