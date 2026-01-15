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

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import jakarta.validation.Constraint;
import jakarta.validation.Payload;
import jakarta.validation.ReportAsSingleViolation;
import jakarta.validation.constraints.Pattern;
import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * Validation constraint that ensures a field contains a valid name.
 *
 * <p>This constraint enforces that the annotated field contains a string that:
 *
 * <ul>
 *   <li>Starts with a letter (uppercase or lowercase)
 *   <li>Contains only letters, numbers, and underscores
 * </ul>
 *
 * <p>This pattern matches the common requirements for identifiers in many programming languages and
 * database systems, ensuring that names are valid across different contexts.
 *
 * <p>The constraint is implemented using the {@link Pattern} annotation with the regular expression
 * {@code ^[A-Za-z]\w*$}, which enforces these rules.
 *
 * <p>Usage example:
 *
 * <pre>
 * public class MyClass {
 *     &#64;ValidName
 *     private String name;
 * }
 * </pre>
 *
 * <p>This constraint is used for validating column names, constant names, and other identifiers
 * throughout the application to ensure they follow a consistent naming convention.
 */
@Pattern(regexp = "^[A-Za-z]\\w*$")
@Constraint(validatedBy = {}) // required, even if no validator class
@Target(FIELD)
@Retention(RUNTIME)
@ReportAsSingleViolation
@Documented
public @interface ValidName {

  /**
   * Error message template to use when the validation fails.
   *
   * <p>The default message includes the pattern that the field must match.
   *
   * @return the error message template
   */
  String message() default "must be a valid name ([A-Za-z]\\w*)";

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
