package au.csiro.pathling.security;

import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;


/**
 * For more info on aspects see: https://docs.spring.io/spring-framework/docs/current/reference/html/core.html#aop-ataspectj-advice-params
 */
@Aspect
@Component
@Profile("core")
@ConditionalOnProperty(prefix = "pathling", name = "auth.enabled", havingValue = "true")
@Slf4j
public class SecurityAspect {

  @Before("@annotation(resourceAccess) && args(resourceType,..)")
  public void checkResourceRead(ResourceAccess resourceAccess, ResourceType resourceType) {
    log.debug("Before access to resource: {}, type: {}", resourceType, resourceAccess.value());
    PathlingAuthority.resourceAccess(resourceType, resourceAccess.value()).checkAuthorised();
  }

  @Before("@annotation(requiresAuthority)")
  public void checkRequiredAuthority(RequiresAuthority requiresAuthority) {
    log.debug("Before call to: {}", requiresAuthority.value());
    PathlingAuthority.fromAuthority(requiresAuthority.value()).checkAuthorised();
  }
}
