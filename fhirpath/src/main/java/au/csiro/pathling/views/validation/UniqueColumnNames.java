package au.csiro.pathling.views.validation;

import jakarta.validation.Constraint;
import jakarta.validation.Payload;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Validation constraint that ensures all Column names within a FhirView are unique.
 */
@Documented
@Constraint(validatedBy = UniqueColumnNamesValidator.class)
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface UniqueColumnNames {
    
    String message() default "Column names must be unique within a view";
    
    Class<?>[] groups() default {};
    
    Class<? extends Payload>[] payload() default {};
}
