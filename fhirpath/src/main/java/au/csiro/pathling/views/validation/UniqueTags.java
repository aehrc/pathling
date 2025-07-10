package au.csiro.pathling.views.validation;

import jakarta.validation.Constraint;
import jakarta.validation.Payload;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Validates that a list of tags does not have more than one tag with any of the specified names.
 */
@Documented
@Constraint(validatedBy = UniqueTagsValidator.class)
@Target({ElementType.FIELD, ElementType.METHOD, ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
public @interface UniqueTags {

  /**
   * The tag names that should be unique within the list.
   *
   * @return array of tag names that should be unique
   */
  String[] value() default {};

  String message() default "List must not contain more than one tag with the same name from the restricted list";

  Class<?>[] groups() default {};

  Class<? extends Payload>[] payload() default {};
}
