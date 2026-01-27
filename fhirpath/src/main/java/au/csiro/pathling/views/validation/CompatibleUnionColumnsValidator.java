/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.csiro.pathling.views.validation;

import au.csiro.pathling.views.Column;
import au.csiro.pathling.views.SelectClause;
import jakarta.annotation.Nonnull;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import java.util.List;
import java.util.stream.Stream;

/**
 * Validator implementation for the {@link CompatibleUnionColumns} constraint.
 *
 * <p>This validator ensures that all elements in a {@code unionAll} list have compatible columns.
 * Columns are considered compatible when they have the same number of columns, and each
 * corresponding column pair is compatible according to {@link Column#isCompatibleWith(Column)}.
 *
 * <p>The validator compares the first element's columns with all other elements in the list. If any
 * incompatibility is found, a detailed constraint violation message is created indicating which
 * element is incompatible.
 */
public class CompatibleUnionColumnsValidator
    implements ConstraintValidator<CompatibleUnionColumns, List<SelectClause>> {

  /**
   * Checks if two lists of columns are compatible with each other.
   *
   * <p>Two lists of columns are considered compatible when:
   *
   * <ol>
   *   <li>They have the same number of columns
   *   <li>Each corresponding column pair is compatible according to {@link
   *       Column#isCompatibleWith(Column)}
   * </ol>
   *
   * @param left the first list of columns to compare
   * @param right the second list of columns to compare
   * @return {@code true} if the lists are compatible, {@code false} otherwise
   */
  static boolean areCompatible(
      @Nonnull final List<Column> left, @Nonnull final List<Column> right) {
    if (left.size() != right.size()) {
      return false; // Different number of columns
    }
    for (int i = 0; i < left.size(); i++) {
      final Column leftColumn = left.get(i);
      final Column rightColumn = right.get(i);
      if (!leftColumn.isCompatibleWith(rightColumn)) {
        return false; // Columns are not compatible
      }
    }
    return true;
  }

  /**
   * Validates that all elements in a list of {@link SelectClause} have compatible columns.
   *
   * <p>The validation process:
   *
   * <ol>
   *   <li>Extracts the column schema from each {@code SelectClause} in the list
   *   <li>Uses the first element's schema as the reference
   *   <li>Compares each subsequent element's schema with the reference
   *   <li>If any incompatibility is found, creates a detailed constraint violation message
   * </ol>
   *
   * <p>Empty or null lists are considered valid (other constraints like {@code @NotNull} should
   * handle those cases).
   *
   * @param value the list of {@link SelectClause} to validate
   * @param context the constraint validator context
   * @return {@code true} if all elements have compatible columns, {@code false} otherwise
   */
  @Override
  public boolean isValid(final List<SelectClause> value, final ConstraintValidatorContext context) {
    if (value == null || value.isEmpty()) {
      return true; // Let @NotNull handle this case
    }

    // create a list of columns for each unionAll element
    final List<List<Column>> unionSchemas =
        value.stream().map(SelectClause::getAllColumns).map(Stream::toList).toList();

    // we already checked value is not empty, so we can safely get the first element
    final List<Column> firstSchema = unionSchemas.get(0);
    // process the rest of unionColumns and check compatibility

    context.disableDefaultConstraintViolation();
    boolean isValid = true;
    for (int i = 1; i < unionSchemas.size(); i++) {
      if (!areCompatible(firstSchema, unionSchemas.get(i))) {
        isValid = false;
        // Create a custom message with the incompatible union
        final String message =
            "Incompatible columns found in unionAll element at index "
                + i
                + ": expected "
                + firstSchema
                + " but found "
                + unionSchemas.get(i).toString();
        context
            .buildConstraintViolationWithTemplate(message)
            .addBeanNode()
            .inIterable()
            .atIndex(i)
            .addConstraintViolation();
      }
    }
    return isValid;
  }
}
