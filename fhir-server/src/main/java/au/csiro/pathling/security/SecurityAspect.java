package au.csiro.pathling.security;

import au.csiro.pathling.errors.AccessDeniedError;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;


/**
 * The aspect defining pathling security. For more info on aspects
 * <p>
 * see: https://docs.spring.io/spring-framework/docs/current/reference/html/core.html#aop-ataspectj-advice-params
 */
@Aspect
@Component
@Profile("core")
@ConditionalOnProperty(prefix = "pathling", name = "auth.enabled", havingValue = "true")
@Slf4j
public class SecurityAspect {

  /**
   * Checks if the current user is authorised to access the resource.
   *
   * @param resourceAccess the resource access required.
   * @param resourceType the resource type.
   * @throws AccessDeniedError if unauthorised.
   */
  @Before("@annotation(resourceAccess) && args(resourceType,..)")
  public void checkResourceRead(ResourceAccess resourceAccess, ResourceType resourceType) {
    log.debug("Before access to resource: {}, type: {}", resourceType, resourceAccess.value());
    checkHasAuthority(PathlingAuthority.resourceAccess(resourceType, resourceAccess.value()));
  }

  /**
   * Checks if the current user is authorised to access the operation.
   *
   * @param operationAccess the operation access required.
   * @throws AccessDeniedError if unauthorised.
   */
  @Before("@annotation(operationAccess)")
  public void checkRequiredAuthority(OperationAccess operationAccess) {
    log.debug("Before call to operation: {}", operationAccess.value());
    checkHasAuthority(PathlingAuthority.operationAccess(operationAccess.value()));
  }

  private static void checkHasAuthority(@Nonnull final PathlingAuthority requiredAuthority) {
    final Authentication authentication = SecurityContextHolder
        .getContext().getAuthentication();
    final AbstractAuthenticationToken authToken = (authentication instanceof AbstractAuthenticationToken)
                                                  ? (AbstractAuthenticationToken) authentication
                                                  : null;
    if (authToken == null || authToken.getAuthorities() == null || !requiredAuthority
        .subsumedByAny(authToken.getAuthorities())) {
      throw new AccessDeniedError(
          String.format("Access denied. Missing authority:`%s`", requiredAuthority.toString()),
          requiredAuthority.getAuthority());
    }
  }
}
