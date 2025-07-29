package au.csiro.pathling.views.validation;

import static java.util.Objects.requireNonNull;

import au.csiro.pathling.views.Column;
import au.csiro.pathling.views.FhirView;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Validator implementation for the {@link UniqueColumnNames} constraint.
 * <p>
 * This validator ensures that all column names within a {@code FhirView} are unique. It works by:
 * <ol>
 *   <li>Collecting all column names from the view using {@link FhirView#getAllColumns()}</li>
 *   <li>Grouping them by name and counting occurrences</li>
 *   <li>Identifying any names that appear more than once</li>
 *   <li>Creating a detailed error message if duplicates are found</li>
 * </ol>
 * <p>
 * The validator produces a custom error message that lists all duplicate column names
 * in alphabetical order, making it easier for users to identify and fix the issues.
 */
public class UniqueColumnNamesValidator implements
    ConstraintValidator<UniqueColumnNames, FhirView> {

  /**
   * Validates that all column names within a {@link FhirView} are unique.
   * <p>
   * The validation process:
   * <ol>
   *   <li>If the view is null, returns true (letting {@code @NotNull} handle that case)</li>
   *   <li>Collects all column names from the view</li>
   *   <li>Groups them by name and counts occurrences</li>
   *   <li>Filters for names that appear more than once</li>
   *   <li>If duplicates are found, creates a custom error message and returns false</li>
   * </ol>
   *
   * @param view the {@link FhirView} to validate
   * @param context the constraint validator context
   * @return {@code true} if all column names are unique, {@code false} otherwise
   */
  @Override
  public boolean isValid(final FhirView view, final ConstraintValidatorContext context) {
    if (view == null) {
      return true; // Let @NotNull handle this case
    }
    final List<String> duplicateNames = requireNonNull(view).getAllColumns()
        .map(Column::getName)
        .filter(Objects::nonNull) // this will be picked up by @NotNull on Column.name but possible
        .collect(Collectors.groupingBy(name -> name, Collectors.counting()))
        .entrySet().stream()
        .filter(entry -> entry.getValue() > 1)
        .map(Map.Entry::getKey)
        .sorted()
        .toList();

    if (!duplicateNames.isEmpty()) {
      // Disable default message
      context.disableDefaultConstraintViolation();
      // Create a custom message with the duplicate names
      final String message = "Duplicate column names found: " + String.join(", ", duplicateNames);
      context.buildConstraintViolationWithTemplate(message)
          .addConstraintViolation();
      return false;
    }
    return true;
  }
}
