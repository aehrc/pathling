/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.security;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.TestPropertySource;

/**
 * @see <a href="https://docs.spring.io/spring-security/site/docs/5.2.x/reference/html/test.html">Spring
 * Security - Testing</a>
 * @see <a href="https://stackoverflow.com/questions/58289509/in-spring-boot-test-how-do-i-map-a-temporary-folder-to-a-configuration-property">In
 * Spring Boot Test, how do I map a temporary folder to a configuration property?</a>
 */
@TestPropertySource(properties = {"pathling.auth.enabled=true"})
@MockBean(OidcConfiguration.class)
@MockBean(JwtDecoder.class)
@MockBean(JwtAuthenticationConverter.class)
public class SecurityEnabledOperationsTest extends SecurityTestForOperations {

  @Test
  @WithMockUser(username = "admin")
  void testForbiddenIfImportWithoutAuthority() {
    assertThrowsAccessDenied(this::assertImportSuccess, "pathling:import");
  }

  @Test
  @WithMockUser(username = "admin", authorities = {"pathling:import"})
  void testPassIfImportWithAuthority() {
    assertImportSuccess();
  }

  @Test
  @WithMockUser(username = "admin")
  void testForbiddenIfAggregateWithoutAuthority() {
    assertThrowsAccessDenied(this::assertAggregateSuccess, "pathling:aggregate");
  }

  @Test
  @WithMockUser(username = "admin", authorities = {"pathling:aggregate"})
  void testPassIfAggregateWithAuthority() {
    assertAggregateSuccess();
  }

  @Test
  @WithMockUser(username = "admin")
  void testForbiddenIfSearchWithoutAuthority() {
    assertThrowsAccessDenied(this::assertSearchSuccess, "pathling:search");
    assertThrowsAccessDenied(this::assertSearchWithFilterSuccess, "pathling:search");
  }

  @Test
  @WithMockUser(username = "admin", authorities = {"pathling:search"})
  void testPassIfSearchWithAuthority() {
    assertSearchSuccess();
  }

  @Test
  @WithMockUser(username = "admin", authorities = {"pathling:create"})
  void testPassIfCreateWithAuthority() {
    assertCreateSuccess();
  }

  @Test
  @WithMockUser(username = "admin", authorities = {"pathling:update"})
  void testPassIfUpdateWithAuthority() {
    assertUpdateSuccess();
  }

  @Test
  @WithMockUser(username = "admin")
  void testForbiddenIfCreateWithoutAuthority() {
    assertThrowsAccessDenied(this::assertCreateSuccess, "pathling:create");
  }

  @Test
  @WithMockUser(username = "admin")
  void testForbiddenIfUpdateWithoutAuthority() {
    assertThrowsAccessDenied(this::assertUpdateSuccess, "pathling:update");
  }

  @Test
  @WithMockUser(username = "admin",
      authorities = {"pathling:batch", "pathling:create", "pathling:update"})
  void testPassIfBatchWithAuthority() {
    assertBatchSuccess();
  }

  @Test
  @WithMockUser(username = "admin", authorities = {"pathling:create", "pathling:update"})
  void testForbiddenIfBatchWithoutBatch() {
    assertThrowsAccessDenied(this::assertBatchSuccess, "pathling:batch");
  }

  @Test
  @WithMockUser(username = "admin", authorities = {"pathling:batch", "pathling:update"})
  void testForbiddenIfBatchWithoutCreate() {
    assertThrowsAccessDenied(this::assertBatchSuccess, "pathling:create");
  }

  @Test
  @WithMockUser(username = "admin", authorities = {"pathling:batch", "pathling:create"})
  void testForbiddenIfBatchWithoutUpdate() {
    assertThrowsAccessDenied(this::assertBatchSuccess, "pathling:update");
  }

}
