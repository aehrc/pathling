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
class SecurityEnabledResourcesTest extends SecurityTestForResources {

  @Test
  @WithMockUser(username = "admin")
  void testForbiddenOnResourceWriteWithoutAuthority() {
    assertThrowsAccessDenied(this::assertWriteSuccess, "pathling:write:Account");
  }

  @Test
  @WithMockUser(username = "admin", authorities = {"pathling:write:Account"})
  void testPassIfResourceWriteWithSpecificAuthority() {
    assertWriteSuccess();
  }

  @Test
  @WithMockUser(username = "admin", authorities = {"pathling:write"})
  void testPassIfResourceWriteWithWildcardAuthority() {
    assertWriteSuccess();
  }

  @Test
  @WithMockUser(username = "admin")
  void testForbiddenOnResourceUpdateWithoutAuthority() {
    assertThrowsAccessDenied(this::assertUpdateSuccess, "pathling:write:Account");
  }

  @Test
  @WithMockUser(username = "admin", authorities = {"pathling:write:Account"})
  void testPassIfResourceUpdateWithSpecificAuthority() {
    assertUpdateSuccess();
  }

  @Test
  @WithMockUser(username = "admin", authorities = {"pathling:write"})
  void testPassIfResourceUpdateWithWildcardAuthority() {
    assertUpdateSuccess();
  }

  @Test
  @WithMockUser(username = "admin")
  void testForbiddenOnResourceReadWithoutAuthority() {
    assertThrowsAccessDenied(this::assertReadSuccess, "pathling:read:Account");
  }

  @Test
  @WithMockUser(username = "admin", authorities = {"pathling:read:Account"})
  void testPassIfResourceReadWithSpecificAuthority() {
    assertReadSuccess();
  }

  @Test
  @WithMockUser(username = "admin", authorities = {"pathling:read"})
  void testPassIfResourceReadWithWildcardAuthority() {
    assertReadSuccess();
  }

}
