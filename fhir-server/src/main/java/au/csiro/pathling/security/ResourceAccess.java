package au.csiro.pathling.security;


import java.lang.annotation.*;

@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
@Documented
public @interface ResourceAccess {

  PathlingAuthority.AccessType value();
}
