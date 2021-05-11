package au.csiro.pathling.security;

import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;
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
public class SecurityAspect {

  @Before("@annotation(resourceReadAccess) && args(resourceType,..)")
  public void checkResourceRead(ResourceReadAccess resourceReadAccess, ResourceType resourceType) {
    System.out.println("###### BEFORE read resource" + resourceType);
    SecurityUtils.checkHasAuthority(PathlingAuthority.readResource(resourceType));
  }

  @Pointcut("execution(* au.csiro.pathling.io.ResourceWriter.write(..))")
  public void writeResource() {
  }

  @Before("writeResource() && args(resourceType,..)")
  public void checkResourceWrite(ResourceType resourceType) {
    System.out.println("###### BEFORE write resource" + resourceType);
    SecurityUtils.checkHasAuthority(PathlingAuthority.writeResource(resourceType));
  }

  @Before("@annotation(requiresAuthority)")
  public void checkRequiredAuthority(RequiresAuthority requiresAuthority) {
    System.out.println(">>>>>> Requires auhtority: " + requiresAuthority.value());
    SecurityUtils.checkHasAuthority(PathlingAuthority.fromString(requiresAuthority.value()));
  }
}
