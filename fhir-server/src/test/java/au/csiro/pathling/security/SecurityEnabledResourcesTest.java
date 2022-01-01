/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.security;

import org.junit.jupiter.api.Test;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.TestPropertySource;

@TestPropertySource(properties = {"pathling.auth.enabled=true"})
public class SecurityEnabledResourcesTest extends SecurityTestForResources {

  @Test
  @WithMockUser(username = "admin")
  public void testForbiddenOnResourceWriteWithoutAuthority() {
    assertThrowsAccessDenied(this::assertWriteSuccess, "pathling:write:Account");
  }

  @Test
  @WithMockUser(username = "admin", authorities = {"pathling:write:Account"})
  public void testPassIfResourceWriteWithSpecificAuthority() {
    assertWriteSuccess();
  }

  @Test
  @WithMockUser(username = "admin", authorities = {"pathling:write"})
  public void testPassIfResourceWriteWithWildcardAuthority() {
    assertWriteSuccess();
  }

  @Test
  @WithMockUser(username = "admin")
  public void testForbiddenOnResourceReadWithoutAuthority() {
    assertThrowsAccessDenied(this::assertReadSuccess, "pathling:read:Account");
  }

  @Test
  @WithMockUser(username = "admin", authorities = {"pathling:read:Account"})
  public void testPassIfResourceReadWithSpecificAuthority() {
    assertReadSuccess();
  }

  @Test
  @WithMockUser(username = "admin", authorities = {"pathling:read"})
  public void testPassIfResourceReadWithWildcardAuthority() {
    assertReadSuccess();
  }
  
}
