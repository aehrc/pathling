package au.csiro.pathling.security;

import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.springframework.stereotype.Component;

@Aspect
@Component
public class SecurityAspect {

  @Pointcut("execution(* au.csiro.pathling.io.ResourceReader.read(..))")
  public void readResource() {
  }

  @Before("readResource() && args(resourceType,..)")
  public void checkResourceRead(ResourceType resourceType) {
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

}
