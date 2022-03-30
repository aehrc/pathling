/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.security;


import au.csiro.pathling.security.PathlingAuthority.AccessType;
import java.lang.reflect.Method;
import java.util.Objects;
import javax.annotation.Nonnull;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.Test;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.util.ReflectionUtils;

class SecurityAspectTest extends SecurityTest {

  @OperationAccess("test")
  @ResourceAccess(AccessType.READ)
  @SuppressWarnings({"unused", "EmptyMethod"})
  void myOperation() {
  }

  @Nonnull
  final Method testOperationMethods = Objects.requireNonNull(ReflectionUtils
      .findMethod(this.getClass(), "myOperation"));

  @Nonnull
  final OperationAccess operationAccess = Objects.requireNonNull(AnnotationUtils
      .getAnnotation(testOperationMethods, OperationAccess.class));

  @Nonnull
  final ResourceAccess resourceAccess = Objects.requireNonNull(AnnotationUtils
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
