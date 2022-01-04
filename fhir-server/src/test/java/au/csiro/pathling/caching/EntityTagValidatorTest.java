/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.caching;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * @author John Grimes
 */
@Tag("UnitTest")
class EntityTagValidatorTest {

  @Test
  void tag() {
    final EntityTagValidator validator = new EntityTagValidator(1630000000000L);
    final String actual = validator.tag();
    assertEquals("W/\"kst7wxds\"", actual);
  }

  @Test
  void tagForTime() {
    final EntityTagValidator validator = new EntityTagValidator();
    final String actual = validator.tagForTime(1630000000001L);
    assertEquals("W/\"kst7wxdt\"", actual);
  }

  @Test
  void matches() {
    final EntityTagValidator validator = new EntityTagValidator(1630000000000L);
    final String matchingTag = "W/\"kst7wxds\"";
    final String notMatchingTag = "W/\"zzkst7wxds\"";
    assertTrue(validator.matches(matchingTag));
    assertFalse(validator.matches(notMatchingTag));
    assertFalse(validator.matches(null));
  }

  @Test
  void expire() {
    final EntityTagValidator validator = new EntityTagValidator(1630000000000L);
    validator.expire(1630000000001L);
    final String actual = validator.tag();
    assertEquals("W/\"kst7wxdt\"", actual);
  }

}
