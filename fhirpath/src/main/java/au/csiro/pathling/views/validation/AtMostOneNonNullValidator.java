package au.csiro.pathling.views.validation;

import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;

/**
 * Validator implementation for the {@link AtMostOneNonNull} constraint.
 * <p>
 * This validator ensures that at most one of the specified fields has a non-null value. It uses
 * reflection to access the field values and count how many are non-null.
 */
public class AtMostOneNonNullValidator implements
    ConstraintValidator<AtMostOneNonNull, Object> {

  private List<String> fieldNames;

  @Override
  public void initialize(final AtMostOneNonNull constraintAnnotation) {
    fieldNames = Arrays.asList(constraintAnnotation.value());
  }

  @Override
  public boolean isValid(final Object value, final ConstraintValidatorContext context) {
    if (value == null) {
      return true; // Let @NotNull handle this case
    }

    int nonNullCount = 0;
    final Class<?> clazz = value.getClass();

    for (final String fieldName : fieldNames) {
      try {
        final Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        if (field.get(value) != null) {
          nonNullCount++;
        }
      } catch (NoSuchFieldException | IllegalAccessException e) {
        // If we can't access the field, we'll assume it's null
        // This shouldn't happen with proper usage of the annotation
      }
    }

    return nonNullCount <= 1;
  }
}
