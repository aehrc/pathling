/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.fhirpath.parser;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import au.csiro.pathling.errors.InvalidUserInputError;
import jakarta.annotation.Nonnull;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ParserTest {

  @Nonnull
  static Stream<Arguments> validExpressions() {
    return Stream.of(
        Arguments.of("@T12:00:00", "Time with hour, minute, second"),
        Arguments.of("@T14:30:14.559", "Time with milliseconds"),
        Arguments.of("@2020-01-01", "Full date with year, month, day"),
        Arguments.of("@2020-01", "Partial date with year and month"),
        Arguments.of("123", "Integer literal"),
        Arguments.of("'hello'", "Simple string literal"),
        Arguments.of("true", "Boolean true literal"),
        Arguments.of("false", "Boolean false literal"),
        Arguments.of("123.456", "Decimal literal"),
        Arguments.of("Patient.name", "Simple resource navigation"),
        Arguments.of("Patient.name.given", "Chained resource navigation"),
        Arguments.of("Patient.name.where(use = 'official')", "Where function with equality check"),
        Arguments.of("Patient.name.first()", "First function"),
        Arguments.of("name.empty()", "Empty function"),
        Arguments.of("name.count()", "Count function"),
        Arguments.of("birthDate > @2000", "Comparison with date literal"),
        Arguments.of("active = true and name.exists()", "Logical operator with function"));
  }

  @Nonnull
  static Stream<Arguments> invalidExpressions() {
    return Stream.of(
        Arguments.of("@T12:00:00XXXXX", "Time with extra characters"),
        Arguments.of("@T12:00:00+01:00", "Time with timezone offset"),
        Arguments.of("'dsdsds", "Unterminated string literal"));
  }

  @ParameterizedTest(name = "{1}")
  @MethodSource("validExpressions")
  void testParse(final String input, final String description) {
    assertEquals(input, new Parser().parse(input).toExpression());
  }

  @ParameterizedTest(name = "{1}")
  @MethodSource("invalidExpressions")
  void testParseErrors(final String input, final String description) {
    final Parser parser = new Parser();
    assertThrows(InvalidUserInputError.class, () -> parser.parse(input));
  }
}
