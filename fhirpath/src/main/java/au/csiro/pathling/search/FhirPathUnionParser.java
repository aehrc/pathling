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

package au.csiro.pathling.search;

import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Parses FHIRPath union expressions from search parameter definitions and maps them to resource
 * types.
 *
 * <p>Search parameters can have union expressions like:
 *
 * <pre>
 * Patient.name.given | Practitioner.name.given
 * </pre>
 *
 * <p>This parser splits such expressions and groups them by resource type. Unqualified expressions
 * (those not starting with a resource name) are included for ALL base resources, as they are meant
 * to work in the context of any resource.
 *
 * @see <a href="https://hl7.org/fhir/searchparameter.html">SearchParameter</a>
 */
public final class FhirPathUnionParser {

  private FhirPathUnionParser() {
    // Utility class - do not instantiate
  }

  /**
   * Splits a FHIRPath union expression and maps parts to resource types.
   *
   * <p>For each base resource type:
   *
   * <ul>
   *   <li>Includes all expressions qualified with that resource type
   *   <li>Includes ALL unqualified expressions (they work in context of any resource)
   * </ul>
   *
   * @param expression the full expression (may contain | unions)
   * @param bases the resource type names this parameter applies to
   * @return map of resource type name to list of expressions for that resource
   */
  @Nonnull
  public static Map<String, List<String>> parse(
      @Nonnull final String expression, @Nonnull final List<String> bases) {

    final List<String> parts = splitUnion(expression);

    // Separate qualified and unqualified expressions.
    final List<String> unqualified = parts.stream().filter(p -> !isQualified(p)).toList();

    final Map<String, List<String>> qualified =
        parts.stream()
            .filter(FhirPathUnionParser::isQualified)
            .collect(Collectors.groupingBy(FhirPathUnionParser::extractResourceType));

    // For each base: combine qualified + all unqualified.
    final Map<String, List<String>> result = new HashMap<>();
    for (final String base : bases) {
      final List<String> exprs = new ArrayList<>();
      exprs.addAll(qualified.getOrDefault(base, List.of()));
      exprs.addAll(unqualified); // ALL unqualified added to every resource.
      if (!exprs.isEmpty()) {
        result.put(base, List.copyOf(exprs));
      }
    }
    return result;
  }

  /**
   * Splits expression by | respecting parentheses nesting.
   *
   * <p>Example: {@code "A | (B | C).first() | D"} → {@code ["A", "(B | C).first()", "D"]}
   *
   * @param expression the expression to split
   * @return list of individual expression parts
   */
  @Nonnull
  static List<String> splitUnion(@Nonnull final String expression) {
    final List<String> parts = new ArrayList<>();
    int depth = 0;
    final StringBuilder current = new StringBuilder();

    for (final char c : expression.toCharArray()) {
      if (c == '(') {
        depth++;
        current.append(c);
      } else if (c == ')') {
        depth--;
        current.append(c);
      } else if (c == '|' && depth == 0) {
        parts.add(current.toString().trim());
        current.setLength(0);
      } else {
        current.append(c);
      }
    }
    parts.add(current.toString().trim());

    return parts.stream().filter(s -> !s.isEmpty()).toList();
  }

  /**
   * Checks if expression is qualified (starts with uppercase = resource name).
   *
   * <p>Handles expressions wrapped in parentheses like {@code (Patient.name)}.
   *
   * @param expr the expression to check
   * @return true if the expression starts with a resource name
   */
  static boolean isQualified(@Nonnull final String expr) {
    final String clean = expr.startsWith("(") ? expr.substring(1) : expr;
    return !clean.isEmpty() && Character.isUpperCase(clean.charAt(0));
  }

  /**
   * Extracts the resource type name from a qualified expression.
   *
   * <p>Example: {@code "Patient.name.given"} → {@code "Patient"}
   *
   * @param expr the qualified expression
   * @return the resource type name (the prefix before the first dot)
   */
  @Nonnull
  static String extractResourceType(@Nonnull final String expr) {
    final String clean = expr.startsWith("(") ? expr.substring(1) : expr;
    return clean.split("\\.")[0];
  }
}
