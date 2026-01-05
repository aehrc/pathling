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
  public void initialize(final UniqueTags constraintAnnotation) {
    uniqueTagNames = Arrays.stream(constraintAnnotation.value()).collect(Collectors.toSet());
  }

  @Override
  public boolean isValid(final List<ColumnTag> tags, final ConstraintValidatorContext context) {
    if (tags == null || tags.isEmpty()) {
      return true; // Empty or null lists are valid
    }

    // Disable default constraint violation
    context.disableDefaultConstraintViolation();

    // Count occurrences of each tag name
    final Map<String, Long> tagNameCounts =
        tags.stream()
            .filter(tag -> uniqueTagNames.contains(tag.getName()))
            .collect(Collectors.groupingBy(ColumnTag::getName, Collectors.counting()));

    // Check if any restricted tag name appears more than once
    boolean isValid = true;
    for (final Map.Entry<String, Long> entry : tagNameCounts.entrySet()) {
      if (entry.getValue() > 1) {
        context
            .buildConstraintViolationWithTemplate(
                "List must not contain more than one '" + entry.getKey() + "' tag")
            .addConstraintViolation();
        isValid = false;
      }
    }

    return isValid;
  }
}
