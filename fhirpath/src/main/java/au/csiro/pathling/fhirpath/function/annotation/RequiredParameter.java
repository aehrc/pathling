package au.csiro.pathling.fhirpath.function.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.jetbrains.annotations.NotNull;

/**
 * Indicates that a method parameter is a required parameter for a FHIRPath function.
 *
 * @author John Grimes
 */
@NotNull
@Target(ElementType.PARAMETER)
@Retention(RetentionPolicy.RUNTIME)
public @interface RequiredParameter {

  /**
   * @return Whether the parameter is expected to be singular
   */
  boolean singular() default false;

}
