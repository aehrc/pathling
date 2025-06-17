package au.csiro.pathling.views.validation;

import au.csiro.pathling.views.Column;
import au.csiro.pathling.views.FhirView;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Validator implementation for the UniqueColumnNames constraint.
 */
public class UniqueColumnNamesValidator implements ConstraintValidator<UniqueColumnNames, FhirView> {

    @Override
    public boolean isValid(FhirView view, ConstraintValidatorContext context) {
        if (view == null) {
            return true; // Let @NotNull handle this case
        }
        final List<String> duplicateNames = requireNonNull(view).getAllColumns()
                .map(Column::getName)
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
            String message = "Duplicate column names found: " + String.join(", ", duplicateNames);
            context.buildConstraintViolationWithTemplate(message)
                    .addConstraintViolation();
            return false;
        }
        return true;
    }
}
