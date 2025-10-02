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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import au.csiro.pathling.errors.AccessDeniedError;
import au.csiro.pathling.util.FhirServerTestConfiguration;
import jakarta.annotation.Nonnull;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.function.Executable;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Profile;
import org.springframework.test.context.ActiveProfiles;

@Tag("unit-test")
@ActiveProfiles("unit-test")
@Import(FhirServerTestConfiguration.class)
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
abstract class SecurityTest {

  static void assertThrowsAccessDenied(@Nonnull final Executable executable,
      @Nonnull final String message) {
    assertThrows(AccessDeniedError.class, executable, message);
  }

  static void assertThrowsAccessDenied(@Nonnull final Executable executable,
      @Nonnull final String message, @Nonnull final String expectedMissingAuthority) {
    final AccessDeniedError ex = assertThrows(AccessDeniedError.class, executable, message);
    assertEquals(expectedMissingAuthority, ex.getMissingAuthority());
  }

}
