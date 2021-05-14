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

  public static void assertThrowsAccessDenied(@Nonnull final String expectedMissingAuthority,
      @Nonnull final Executable executable) {
    final AccessDeniedError ex = assertThrows(AccessDeniedError.class,
        executable);
    assertEquals(expectedMissingAuthority, ex.getMissingAuthority());
  }
}
