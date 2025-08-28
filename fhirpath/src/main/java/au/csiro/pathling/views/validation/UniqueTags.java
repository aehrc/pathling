/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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

  /**
   * The validation error message.
   *
   * @return the error message
   */
  String message() default "List must not contain more than one tag with the same name from the restricted list";

  /**
   * The validation groups.
   *
   * @return the validation groups
   */
  Class<?>[] groups() default {};

  /**
   * The validation payload.
   *
   * @return the validation payload
   */
  Class<? extends Payload>[] payload() default {};
}
