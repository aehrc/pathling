/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.security;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import au.csiro.pathling.errors.AccessDeniedError;
import javax.annotation.Nonnull;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.function.Executable;
import org.springframework.boot.test.context.SpringBootTest;

@Tag("UnitTest")
@SpringBootTest
public abstract class SecurityTest {

  public static void assertThrowsAccessDenied(@Nonnull final Executable executable,
      @Nonnull final String message) {
    assertThrows(AccessDeniedError.class, executable, message);
  }

  public static void assertThrowsAccessDenied(@Nonnull final Executable executable,
      @Nonnull final String message, @Nonnull final String expectedMissingAuthority) {
    final AccessDeniedError ex = assertThrows(AccessDeniedError.class, executable, message);
    assertEquals(expectedMissingAuthority, ex.getMissingAuthority());
  }

}
