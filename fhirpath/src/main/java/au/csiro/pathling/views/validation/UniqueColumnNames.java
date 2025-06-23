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
 * <p>
 * This constraint is applied at the class level to {@code FhirView} objects to validate
 * that all columns defined within the view have unique names. This is essential for:
 * <ul>
 *   <li>Preventing ambiguity when referencing columns by name</li>
 *   <li>Ensuring valid SQL generation (as SQL requires unique column names)</li>
 *   <li>Maintaining data integrity in the view definition</li>
 * </ul>
 * <p>
 * The validation is performed by {@link UniqueColumnNamesValidator}, which collects
 * all column names from the view and checks for duplicates. If duplicates are found,
 * a detailed error message is generated listing the duplicate column names.
 *
 * @see UniqueColumnNamesValidator
 * @see au.csiro.pathling.views.FhirView
 * @see au.csiro.pathling.views.Column
 */
@Documented
@Constraint(validatedBy = UniqueColumnNamesValidator.class)
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface UniqueColumnNames {
    
    /**
     * Error message template to use when the validation fails.
     * <p>
     * The default message is overridden by the validator with a more specific message
     * that includes details about which column names are duplicated.
     *
     * @return the error message template
     */
    String message() default "Column names must be unique within a view";
    
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
