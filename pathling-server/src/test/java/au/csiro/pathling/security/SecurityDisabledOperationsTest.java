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
import org.springframework.test.context.TestPropertySource;


/**
 * @see <a
 * href="https://docs.spring.io/spring-security/site/docs/5.2.x/reference/html/test.html">Spring
 * Security - Testing</a>
 * @see <a
 * href="https://stackoverflow.com/questions/58289509/in-spring-boot-test-how-do-i-map-a-temporary-folder-to-a-configuration-property">In
 * Spring Boot Test, how do I map a temporary folder to a configuration property?</a>
 */
@TestPropertySource(properties = {"pathling.auth.enabled=false"})
class SecurityDisabledOperationsTest extends SecurityTestForOperations {

  // @Test
  // void testPassIfImportWithNoAuth() {
  //   assertImportSuccess();
  // }
  //
  // @Test
  // void testPassIfAggregateWithNoAuth() {
  //   assertAggregateSuccess();
  // }
  //
  // @Test
  // void testPassIfSearchWithNoAuth() {
  //   assertSearchSuccess();
  //   assertSearchWithFilterSuccess();
  // }
  //
  // @Test
  // void testPassIfUpdateWithNoAuth() {
  //   assertUpdateSuccess();
  // }
  //
  // @Test
  // void testPassIfBatchWithNoAuth() {
  //   assertBatchSuccess();
  // }

}
