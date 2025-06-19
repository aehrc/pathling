package au.csiro.pathling.views.validation;

import jakarta.validation.Constraint;
import jakarta.validation.Payload;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Validation constraint that ensures all elements in unionAll lists have compatible columns.
 * <p>
 * This constraint is used to validate that when multiple {@code SelectClause} objects are combined
 * in a union operation, they all have compatible column structures. Compatibility requires:
 * <ul>
 *   <li>The same number of columns in each element</li>
 *   <li>Compatible data types for corresponding columns</li>
 *   <li>Matching collection indicators (whether a column represents a collection or a single value)</li>
 * </ul>
 * <p>
 * When applied to a field of type {@code List<SelectClause>}, this constraint ensures that
 * all elements can be safely combined in a SQL UNION ALL operation without type conflicts.
 * <p>
 * The validation is performed by {@link CompatibleUnionColumnsValidator}, which compares
 * the first element's columns with all other elements in the list.
 *
 * @see CompatibleUnionColumnsValidator
 */
@Documented
@Constraint(validatedBy = CompatibleUnionColumnsValidator.class)
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface CompatibleUnionColumns {
    
    /**
     * Error message template to use when the validation fails.
     * <p>
     * The default message is overridden by the validator with a more specific message
     * that includes details about which element is incompatible and why.
     *
     * @return the error message template
     */
    String message() default "All elements in unionAll must have compatible columns";
    
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
