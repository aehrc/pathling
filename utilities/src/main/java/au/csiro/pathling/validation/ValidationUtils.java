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

package au.csiro.pathling.validation;

import static java.util.Objects.nonNull;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import java.util.Set;
import java.util.stream.Collectors;
import org.hibernate.validator.messageinterpolation.ParameterMessageInterpolator;

/** Utility classes to facilitate JRS-380 based validation. */
public final class ValidationUtils {

  private ValidationUtils() {
    // utility class
  }

  // We use the ParameterMessageInterpolator rather than the default one which depends on EL
  // implementation
  // for message interpolation as it causes library conflicts in Databricks environments.
  private static final ValidatorFactory DEFAULT_VALIDATION_FACTORY =
      Validation.byDefaultProvider()
          .configure()
          .messageInterpolator(new ParameterMessageInterpolator())
          .buildValidatorFactory();

  /**
   * Validates a bean annotated with JSR-380 constraints using the default validation factory.
   *
   * @param bean the bean to validate
   * @param <T> the type of the bean.
   * @return the set of violated constrains, empty if the bean is valid.
   */
  @Nonnull
  public static <T> Set<ConstraintViolation<T>> validate(@Nonnull final T bean) {
    final Validator validator = DEFAULT_VALIDATION_FACTORY.getValidator();
    return validator.validate(bean);
  }

  /**
   * Ensures that a bean annotated with JSR-380 constraints is valid. If validation with the default
   * validation factory results in any violation throws the {@link ConstraintViolationException}.
   *
   * @param bean the bean to validate
   * @param message the message to use as the title of the exception message.
   * @param <T> the type of the bean.
   * @return the valid bean.
   * @throws ConstraintViolationException if any constraints are violated.
   */
  @SuppressWarnings("UnusedReturnValue")
  @Nonnull
  public static <T> T ensureValid(@Nonnull final T bean, @Nonnull final String message)
      throws ConstraintViolationException {
    final Set<ConstraintViolation<T>> constraintViolations = validate(bean);
    if (!constraintViolations.isEmpty()) {
      failValidation(constraintViolations, message);
    }
    return bean;
  }

  /**
   * Fails with the {@link ConstraintViolationException} that includes the violated constraints and
   * the human-readable representation of them.
   *
   * @param constraintViolations the violation to include in the exception.
   * @param messageTitle the title of the error message.
   */
  public static void failValidation(
      @Nonnull final Set<? extends ConstraintViolation<?>> constraintViolations,
      @Nullable final String messageTitle)
      throws ConstraintViolationException {
    final String exceptionMessage =
        nonNull(messageTitle)
            ? messageTitle + ": " + formatViolations(constraintViolations)
            : formatViolations(constraintViolations);
    throw new ConstraintViolationException(exceptionMessage, constraintViolations);
  }

  /**
   * Formats a set of {@link ConstraintViolation} to a human-readable string.
   *
   * @param constraintViolations the violations to include
   * @return the human-readable representation of the violations
   */
  @Nonnull
  public static String formatViolations(
      @Nonnull final Set<? extends ConstraintViolation<?>> constraintViolations) {
    return constraintViolations.stream()
        .map(cv -> cv == null ? "null" : cv.getPropertyPath() + ": " + cv.getMessage())
        .sorted()
        .collect(Collectors.joining(", "));
  }
}
