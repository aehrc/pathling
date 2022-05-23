/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.sql.dates;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;

import au.csiro.pathling.errors.InvalidUserInputError;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("UnitTest")
class TemporalDifferenceFunctionTest {

  TemporalDifferenceFunction function;

  @BeforeEach
  void setUp() {
    function = new TemporalDifferenceFunction();
  }

  @Test
  void nullCalendarDuration() {
    final InvalidUserInputError error = assertThrows(InvalidUserInputError.class,
        () -> function.call("2020-01-01", "2020-01-01", null));
    assertEquals("Calendar duration must be provided",
        error.getMessage());
  }

  @Test
  void invalidCalendarDuration() {
    final InvalidUserInputError error = assertThrows(InvalidUserInputError.class,
        () -> function.call("2020-01-01", "2020-01-01", "femtoseconds"));
    assertEquals("Invalid calendar duration: femtoseconds",
        error.getMessage());
  }

}