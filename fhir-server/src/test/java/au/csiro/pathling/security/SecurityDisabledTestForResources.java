package au.csiro.pathling.security;

import org.junit.jupiter.api.Test;
import org.springframework.test.context.TestPropertySource;


@TestPropertySource(
    properties = {
        "pathling.auth.enabled=false",
        "pathling.caching.enabled=false"
    })
public class SecurityDisabledTestForResources extends SecurityTestForResources {

  @Test
  public void testPassIfResourceWriteWithNoAuth() {
    assertWriteSuccess();
  }

  @Test
  public void testPassIfResourceReadWithNoAuth() {
    assertReadSuccess();
  }
}
