package au.csiro.pathling.views.validation;

import au.csiro.pathling.views.Column;
import au.csiro.pathling.views.SelectClause;
import jakarta.annotation.Nonnull;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import java.util.List;
import java.util.stream.Stream;

/**
 * Validator implementation for the CompatibleUnionColumns constraint.
 */
public class CompatibleUnionColumnsValidator implements
    ConstraintValidator<CompatibleUnionColumns, List<SelectClause>> {

  static boolean areCompatible(@Nonnull final List<Column> left,
      @Nonnull final List<Column> right) {
    if (left.size() != right.size()) {
      return false; // Different number of columns
    }
    for (int i = 0; i < left.size(); i++) {
      Column leftColumn = left.get(i);
      Column rightColumn = right.get(i);
      if (!leftColumn.isCompatibleWith(rightColumn)) {
        return false; // Columns are not compatible
      }
    }
    return true;
  }

  @Override
  public boolean isValid(final List<SelectClause> value, final ConstraintValidatorContext context) {
    if (value == null || value.isEmpty()) {
      return true; // Let @NotNull handle this case
    }

    // create a list of columns for each unionAll element
    final List<List<Column>> unionSchemas = value.stream()
        .map(SelectClause::getAllColumns)
        .map(Stream::toList)
        .toList();

    // we already checked value is not empty, so we can safely get the first element
    final List<Column> firstSchema = unionSchemas.get(0);
    // process the rest of unionColumns and check compatibility

    context.disableDefaultConstraintViolation();
    boolean isValid = true;
    for (int i = 1; i < unionSchemas.size(); i++) {
      if (!areCompatible(firstSchema, unionSchemas.get(i))) {
        isValid = false;
        // Create a custom message with the incompatible union
        String message = "Incompatible columns found in unionAll element at index " + i + ":" +
            " expected " + firstSchema + " but found " +
            unionSchemas.get(i).toString();
        context.buildConstraintViolationWithTemplate(message)
            .addBeanNode()
            .inIterable().atIndex(i)
            .addConstraintViolation();
      }
    }
    return isValid;
  }

}
