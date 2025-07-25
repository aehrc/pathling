package au.csiro.pathling.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to indicate that a method is called by name with reflection. This can be useful for
 * tools or frameworks that rely on reflection to discover methods.
 *
 * @author Piotr Szul
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface UsedByReflection {

}
