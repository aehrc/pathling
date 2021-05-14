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

public class SecurityAspectTest extends SecurityTest {

  @OperationAccess("test")
  @ResourceAccess(AccessType.READ)
  public void testOperation() {
  }

  @Nonnull
  private final Method testOperationMethods = Objects.requireNonNull(ReflectionUtils
      .findMethod(this.getClass(), "testOperation"));

  @Nonnull
  private final OperationAccess operationAccess = Objects.requireNonNull(AnnotationUtils
      .getAnnotation(testOperationMethods, OperationAccess.class));

  @Nonnull
  private final ResourceAccess resourceAccess = Objects.requireNonNull(AnnotationUtils
      .getAnnotation(testOperationMethods, ResourceAccess.class));


  private final SecurityAspect securityAspect = new SecurityAspect();

  @Test
  public void testOperationAccessDeniedWhenNoAuthenticatio() {
    assertThrowsAccessDenied("operation:test",
        () -> securityAspect.checkRequiredAuthority(operationAccess));

  }

  @Test
  @WithMockUser(username = "admin")
  public void testOperationAccessDeniedWhenNotAuthorized() {
    assertThrowsAccessDenied("operation:test",
        () -> securityAspect.checkRequiredAuthority(operationAccess));

  }

  @Test
  @WithMockUser(username = "admin", authorities = {"operation:test"})
  public void testOperationAccessGranted() {
    // PASS
    securityAspect.checkRequiredAuthority(operationAccess);
  }


  @Test
  public void testResourceAccessDeniedWhenNoAuthentication() {
    assertThrowsAccessDenied("user/Patient.read",
        () -> securityAspect.checkResourceRead(resourceAccess, ResourceType.PATIENT));
  }

  @Test
  @WithMockUser(username = "admin")
  public void testResourceAccessDeniedWhenNotAuthorized() {
    assertThrowsAccessDenied("user/Patient.read",
        () -> securityAspect.checkResourceRead(resourceAccess, ResourceType.PATIENT));
  }

  @Test
  @WithMockUser(username = "admin", authorities = {"user/Patient.read"})
  public void testResourceAccessGranted() {
    // PASS
    securityAspect.checkResourceRead(resourceAccess, ResourceType.PATIENT);
  }
}
