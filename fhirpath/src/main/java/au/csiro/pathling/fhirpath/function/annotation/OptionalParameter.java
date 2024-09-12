package au.csiro.pathling.fhirpath.function.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.jetbrains.annotations.Nullable;

/**
 * Indicates that a method parameter is an optional parameter for a FHIRPath function.
 *
 * @author John Grimes
 */
@Nullable
@Target(ElementType.PARAMETER)
@Retention(RetentionPolicy.RUNTIME)
public @interface OptionalParameter {

  /**
   * @return Whether the parameter is expected to be singular
   */
  boolean singular() default false;

}
