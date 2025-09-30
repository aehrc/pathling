/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.security;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import java.util.UUID;

@TestPropertySource(properties = {"pathling.auth.enabled=true"})
@Disabled("API has changed, is this test even applicable now?")
class SecurityEnabledResourcesTest extends SecurityTestForResources {

  @DynamicPropertySource
  private static void registerProps(DynamicPropertyRegistry registry) {
    SecurityTestForResources.registerPropertiesFromChildClasses(registry, UUID.randomUUID());
  }
  
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
