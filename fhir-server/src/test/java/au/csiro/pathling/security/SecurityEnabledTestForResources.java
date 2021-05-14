package au.csiro.pathling.security;

import org.junit.jupiter.api.Test;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.TestPropertySource;


@TestPropertySource(
    properties = {
        "pathling.auth.enabled=true",
        "pathling.caching.enabled=false"
    })
public class SecurityEnabledTestForResources extends SecurityTestForResources {


  @Test
  @WithMockUser(username = "admin")
  public void testForbidenIfResourceWritedWithoutAuthority() {
    assertThrowsAccessDenied("user/Account.write",
        this::assertWriteSuccess);
  }

  @Test
  @WithMockUser(username = "admin", authorities = {"user/Account.write"})
  public void testPassIfResourceWriteWithSpecficAuthority() {
    assertWriteSuccess();
  }

  @Test
  @WithMockUser(username = "admin", authorities = {"user/*.write"})
  public void testPassIfResourceWriteWithWildcardAuthority() {
    assertWriteSuccess();
  }

  @Test
  @WithMockUser(username = "admin")
  public void testForbidenIfResourceReadWithoutAuthority() {
    assertThrowsAccessDenied("user/Account.read",
        this::assertReadSuccess);
  }

  @Test
  @WithMockUser(username = "admin", authorities = {"user/Account.read"})
  public void testPassIfResourceReadWithSpecficAuthority() {
    assertReadSuccess();
  }

  @Test
  @WithMockUser(username = "admin", authorities = {"user/*.read"})
  public void testPassIfResourceReadWithWildcardAuthority() {
    assertReadSuccess();
  }
}
