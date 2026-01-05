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

package au.csiro.pathling;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import au.csiro.pathling.errors.InvalidUserInputError;
import java.util.List;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.junit.jupiter.api.Test;

/**
 * Tests for ParamUtil.
 *
 * @author John Grimes
 */
class ParamUtilTest {

  /**
   * Tests that when a parameter has the wrong type (e.g., CodeType when Coding is expected), an
   * InvalidUserInputError is thrown rather than a ClassCastException. This ensures the error
   * results in a 400 Bad Request response rather than a 500 Internal Server Error.
   */
  @Test
  void throwsInvalidUserInputErrorOnTypeMismatch() {
    // Create a parameter with CodeType value.
    final ParametersParameterComponent param = new ParametersParameterComponent();
    param.setName("testParam");
    param.setValue(new CodeType("testValue"));

    // Attempt to extract as Coding (wrong type) - should throw InvalidUserInputError, not
    // ClassCastException.
    assertThatThrownBy(
            () ->
                ParamUtil.extractFromPart(
                    List.of(param),
                    "testParam",
                    Coding.class,
                    Coding::getCode,
                    false,
                    new InvalidUserInputError("Invalid parameter type")))
        .isInstanceOf(InvalidUserInputError.class)
        .hasMessageContaining("Invalid parameter type");
  }

  /**
   * Tests that when a parameter has the wrong type and an onError exception is provided, that
   * exception is thrown with proper cause information.
   */
  @Test
  void throwsProvidedOnErrorExceptionOnTypeMismatch() {
    // Create a parameter with Coding value.
    final ParametersParameterComponent param = new ParametersParameterComponent();
    param.setName("testParam");
    param.setValue(new Coding().setCode("testValue"));

    // Attempt to extract as CodeType (wrong type) - should throw the provided onError exception.
    final InvalidUserInputError expectedError = new InvalidUserInputError("Custom error message");
    assertThatThrownBy(
            () ->
                ParamUtil.extractFromPart(
                    List.of(param),
                    "testParam",
                    CodeType.class,
                    CodeType::getCode,
                    false,
                    expectedError))
        .isInstanceOf(InvalidUserInputError.class)
        .hasMessageContaining("Custom error message");
  }

  /**
   * Tests that when a type mismatch occurs and no onError is provided, a generic
   * InvalidUserInputError is thrown.
   */
  @Test
  void throwsGenericInvalidUserInputErrorWhenNoOnErrorProvided() {
    // Create a parameter with CodeType value.
    final ParametersParameterComponent param = new ParametersParameterComponent();
    param.setName("testParam");
    param.setValue(new CodeType("testValue"));

    // Attempt to extract as Coding with no onError - should throw InvalidUserInputError.
    assertThatThrownBy(
            () ->
                ParamUtil.extractFromPart(
                    List.of(param), "testParam", Coding.class, Coding::getCode, true, null, false))
        .isInstanceOf(InvalidUserInputError.class);
  }

  /** Tests that extraction works correctly when the parameter has the expected type. */
  @Test
  void extractsValueWhenTypeMatches() {
    // Create a parameter with CodeType value.
    final ParametersParameterComponent param = new ParametersParameterComponent();
    param.setName("testParam");
    param.setValue(new CodeType("expectedValue"));

    // Extract as CodeType (correct type) - should succeed.
    final String result =
        ParamUtil.extractFromPart(
            List.of(param),
            "testParam",
            CodeType.class,
            CodeType::getCode,
            false,
            new InvalidUserInputError("Should not be thrown"));

    assertThat(result).isEqualTo("expectedValue");
  }
}
