package au.csiro.pathling.security;


import java.lang.annotation.*;

/**
 * Identifies methods that require certain granted authority to be executed. Used by {@link
 * SecurityAspect} to enforce Resource access authorisation.
 */
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
@Documented
public @interface OperationAccess {

  /**
   * The name of the operation being accessed.
   *
   * @return The name of the the operation.
   */
  String value();
}
