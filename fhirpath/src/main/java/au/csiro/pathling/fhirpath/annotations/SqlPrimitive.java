package au.csiro.pathling.fhirpath.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to indicate that a class represents a primitive SQL type in the context of FHIRPath.
 * This annotation can be used for classes that are intended to handle primitive data types in the
 * context of FHIRPath processing.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface SqlPrimitive {

}
