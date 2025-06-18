package au.csiro.pathling.views.validation;

import jakarta.validation.Constraint;
import jakarta.validation.Payload;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Validation constraint that ensures all elements in unionAll lists have the same columns
 * with regard to column names, types, and collection indicators.
 */
@Documented
@Constraint(validatedBy = CompatibleUnionColumnsValidator.class)
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface CompatibleUnionColumns {
    
    String message() default "All elements in unionAll must have compatible columns";
    
    Class<?>[] groups() default {};
    
    Class<? extends Payload>[] payload() default {};
}
