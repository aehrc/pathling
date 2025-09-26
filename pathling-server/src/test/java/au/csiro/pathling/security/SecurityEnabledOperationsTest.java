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

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.TestPropertySource;

/**
 * @see <a
 * href="https://docs.spring.io/spring-security/site/docs/5.2.x/reference/html/test.html">Spring
 * Security - Testing</a>
 * @see <a
 * href="https://stackoverflow.com/questions/58289509/in-spring-boot-test-how-do-i-map-a-temporary-folder-to-a-configuration-property">In
 * Spring Boot Test, how do I map a temporary folder to a configuration property?</a>
 */
@TestPropertySource(properties = {"pathling.auth.enabled=true",
    "pathling.auth.issuer=https://pathling.acme.com/fhir"})
@MockBean(OidcConfiguration.class)
@MockBean(JwtDecoder.class)
@MockBean(JwtAuthenticationConverter.class)
class SecurityEnabledOperationsTest extends SecurityTestForOperations {

  // @Test
  // @WithMockUser(username = "admin")
  // void testForbiddenIfImportWithoutAuthority() {
  //   assertThrowsAccessDenied(this::assertImportSuccess, "pathling:import");
  // }
  //
  // @Test
  // @WithMockUser(username = "admin", authorities = {"pathling:import"})
  // void testPassIfImportWithAuthority() {
  //   assertImportSuccess();
  // }
  //
  // @Test
  // @WithMockUser(username = "admin")
  // void testForbiddenIfAggregateWithoutAuthority() {
  //   assertThrowsAccessDenied(this::assertAggregateSuccess, "pathling:aggregate");
  // }
  //
  // @Test
  // @WithMockUser(username = "admin", authorities = {"pathling:aggregate"})
  // void testPassIfAggregateWithAuthority() {
  //   assertAggregateSuccess();
  // }
  //
  // @Test
  // @WithMockUser(username = "admin")
  // void testForbiddenIfSearchWithoutAuthority() {
  //   assertThrowsAccessDenied(this::assertSearchSuccess, "pathling:search");
  //   assertThrowsAccessDenied(this::assertSearchWithFilterSuccess, "pathling:search");
  // }
  //
  // @Test
  // @WithMockUser(username = "admin", authorities = {"pathling:search"})
  // void testPassIfSearchWithAuthority() {
  //   assertSearchSuccess();
  // }
  //
  // @Test
  // @WithMockUser(username = "admin", authorities = {"pathling:update"})
  // void testPassIfUpdateWithAuthority() {
  //   assertUpdateSuccess();
  // }
  //
  // @Test
  // @WithMockUser(username = "admin")
  // void testForbiddenIfUpdateWithoutAuthority() {
  //   assertThrowsAccessDenied(this::assertUpdateSuccess, "pathling:update");
  // }
  //
  // @Test
  // @WithMockUser(username = "admin",
  //     authorities = {"pathling:batch", "pathling:update"})
  // void testPassIfBatchWithAuthority() {
  //   assertBatchSuccess();
  // }
  //
  // @Test
  // @WithMockUser(username = "admin", authorities = {"pathling:update"})
  // void testForbiddenIfBatchWithoutBatch() {
  //   assertThrowsAccessDenied(this::assertBatchSuccess, "pathling:batch");
  // }
  //
  // @Test
  // @WithMockUser(username = "admin", authorities = {"pathling:batch"})
  // void testForbiddenIfBatchWithoutUpdate() {
  //   assertThrowsAccessDenied(this::assertBatchSuccess, "pathling:update");
  // }

}
