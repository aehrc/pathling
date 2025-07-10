package au.csiro.pathling.views.validation;

import au.csiro.pathling.views.ColumnTag;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Validator that checks a list of tags does not have more than one tag with any of the specified
 * names.
 */
public class UniqueTagsValidator implements ConstraintValidator<UniqueTags, List<ColumnTag>> {

  private Set<String> uniqueTagNames;

  @Override
  public void initialize(UniqueTags constraintAnnotation) {
    uniqueTagNames = Arrays.stream(constraintAnnotation.value())
        .collect(Collectors.toSet());
  }

  @Override
  public boolean isValid(List<ColumnTag> tags, ConstraintValidatorContext context) {
    if (tags == null || tags.isEmpty()) {
      return true; // Empty or null lists are valid
    }

    // Disable default constraint violation
    context.disableDefaultConstraintViolation();

    // Count occurrences of each tag name
    Map<String, Long> tagNameCounts = tags.stream()
        .filter(tag -> uniqueTagNames.contains(tag.getName()))
        .collect(Collectors.groupingBy(ColumnTag::getName, Collectors.counting()));

    // Check if any restricted tag name appears more than once
    boolean isValid = true;
    for (Map.Entry<String, Long> entry : tagNameCounts.entrySet()) {
      if (entry.getValue() > 1) {
        context.buildConstraintViolationWithTemplate(
                "List must not contain more than one '" + entry.getKey() + "' tag")
            .addConstraintViolation();
        isValid = false;
      }
    }

    return isValid;
  }
}
