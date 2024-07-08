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


import static java.util.Objects.requireNonNull;

import jakarta.annotation.Nonnull;
import java.lang.reflect.Method;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.Test;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.util.ReflectionUtils;

class SecurityAspectTest extends SecurityTest {

  @OperationAccess("test")
  @ResourceAccess(ResourceAccess.AccessType.READ)
  @SuppressWarnings({"unused", "EmptyMethod"})
  void myOperation() {
  }

  @Nonnull
  final Method testOperationMethods = requireNonNull(ReflectionUtils
      .findMethod(this.getClass(), "myOperation"));

  @Nonnull
  final OperationAccess operationAccess = requireNonNull(AnnotationUtils
      .getAnnotation(testOperationMethods, OperationAccess.class));

  @Nonnull
  final ResourceAccess resourceAccess = requireNonNull(AnnotationUtils
      .getAnnotation(testOperationMethods, ResourceAccess.class));


  final SecurityAspect securityAspect = new SecurityAspect();

  @Test
  void testOperationAccessDeniedWhenNoAuthentication() {
    assertThrowsAccessDenied(() -> securityAspect.checkRequiredAuthority(operationAccess),
        "Token not present"
    );

  }

  @Test
  @WithMockUser(username = "admin")
  void testOperationAccessDeniedWhenNotAuthorized() {
    assertThrowsAccessDenied(() -> securityAspect.checkRequiredAuthority(operationAccess),
        "Missing authority: 'pathling:test'", "pathling:test"
    );

  }

  @Test
  @WithMockUser(username = "admin", authorities = {"pathling:test"})
  void testOperationAccessGranted() {
    // PASS
    securityAspect.checkRequiredAuthority(operationAccess);
  }


  @Test
  void testResourceAccessDeniedWhenNoAuthentication() {
    assertThrowsAccessDenied(
        () -> securityAspect.checkResourceRead(resourceAccess, ResourceType.PATIENT),
        "Token not present"
    );
  }

  @Test
  @WithMockUser(username = "admin")
  void testResourceAccessDeniedWhenNotAuthorized() {
    assertThrowsAccessDenied(
        () -> securityAspect.checkResourceRead(resourceAccess, ResourceType.PATIENT),
        "Missing authority: 'pathling:read:Patient'", "pathling:read:Patient"
    );
  }

  @Test
  @WithMockUser(username = "admin", authorities = {"pathling:read:Patient"})
  void testResourceAccessGranted() {
    // PASS
    securityAspect.checkResourceRead(resourceAccess, ResourceType.PATIENT);
  }

}
