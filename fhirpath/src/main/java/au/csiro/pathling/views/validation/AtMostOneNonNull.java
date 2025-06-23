package au.csiro.pathling.views.validation;

import jakarta.validation.Constraint;
import jakarta.validation.Payload;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Validation constraint that ensures at most one of the specified fields is non-null.
 * <p>
 * This constraint is used to validate that among a set of fields, at most one can have a non-null value.
 * It's particularly useful for mutually exclusive options where setting multiple fields would create
 * ambiguity or invalid configurations.
 * <p>
 * The validation is performed by {@link AtMostOneNonNullValidator}, which checks that
 * at most one of the specified fields has a non-null value.
 */
@Documented
@Constraint(validatedBy = AtMostOneNonNullValidator.class)
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface AtMostOneNonNull {
    
    /**
     * The names of the fields that should have at most one non-null value.
     * 
     * @return the field names
     */
    String[] value();
    
    /**
     * Error message template to use when the validation fails.
     *
     * @return the error message template
     */
    String message() default "Only one of the fields {value} can be non-null";
    
    /**
     * The validation groups this constraint belongs to.
     *
     * @return the validation groups
     */
    Class<?>[] groups() default {};
    
    /**
     * Payload that can be attached to a constraint declaration.
     *
     * @return the payload
     */
    Class<? extends Payload>[] payload() default {};
}
