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

package au.csiro.pathling.fhirpath.literal;

import static au.csiro.pathling.fhirpath.literal.StringLiteral.escapeFhirPathString;
import static au.csiro.pathling.fhirpath.literal.StringLiteral.unescapeFhirPathString;
import static au.csiro.pathling.utilities.Strings.unSingleQuote;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Arrays;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.hl7.fhir.r4.model.Coding;

/**
 * Represents a FHIRPath Coding literal.
 *
 * @author John Grimes
 */
public abstract class CodingLiteral {

  /**
   * Special characters that require quoting within a Coding literal component.
   */
  private static final String SPECIAL_CHARACTERS = "\\s'|\\r\\n\\t(),";

  private static final String COMPONENT_REGEX = String
      .format("('.*?(?<!\\\\)'|[^%s]*)", SPECIAL_CHARACTERS);

  private static final Pattern CODING_PATTERN = Pattern
      .compile(String
          .format("%s\\|%s(?:\\|%s)?(?:\\|%s)?(?:\\|%s)?", COMPONENT_REGEX, COMPONENT_REGEX,
              COMPONENT_REGEX, COMPONENT_REGEX, COMPONENT_REGEX));

  private static final Pattern NEEDS_QUOTING = Pattern
      .compile(String.format("[%s]", SPECIAL_CHARACTERS));

  private static final Pattern NEEDS_UNQUOTING = Pattern.compile("^'(.*)'$");

  /**
   * Returns a new instance, parsed from a FHIRPath literal.
   *
   * @param fhirPath The FHIRPath representation of the literal
   * @return A new instance of {@link Coding}
   * @throws IllegalArgumentException if the literal is malformed
   */
  @Nonnull
  public static Coding fromString(@Nonnull final CharSequence fhirPath)
      throws IllegalArgumentException {
    final Matcher matcher = CODING_PATTERN.matcher(fhirPath);
    if (matcher.matches()) {
      final Coding coding = new Coding(decodeComponent(matcher.group(1)),
          decodeComponent(matcher.group(2)), null);
      if (Objects.nonNull(matcher.group(3))) {
        coding.setVersion(decodeComponent(matcher.group(3)));
      }
      if (Objects.nonNull(matcher.group(4))) {
        coding.setDisplay(decodeComponent(matcher.group(4)));
      }
      if (Objects.nonNull(matcher.group(5)) && !matcher.group(5).isBlank()) {
        coding.setUserSelected(Boolean.parseBoolean(decodeComponent(matcher.group(5))));
      }
      return coding;
    } else {
      {
        throw new IllegalArgumentException(
            "Coding literal must be of form: <system>|<code>[|<version>][|<display>[|<userSelected>]]].");
      }
    }
  }

  /**
   * Returns a FHIRPath literal representation of a coding.
   *
   * @param coding A coding to represent as a literal.
   * @return The FHIRPath representation of the coding.
   * @throws IllegalArgumentException if the literal is malformed
   */
  @Nonnull
  public static String toLiteral(@Nonnull final Coding coding) {
    final String[] components = new String[]{
        coding.getSystem(),
        coding.getCode(),
        coding.getVersion(),
        coding.getDisplay(),
        coding.hasUserSelected()
        ? String.valueOf(coding.getUserSelected())
        : null
    };

    // Drop the null components other than system and code from the tail.
    int nonNullHead = components.length;
    while (nonNullHead > 2 && components[nonNullHead - 1] == null) {
      nonNullHead--;
    }
    return Arrays.stream(components)
        .limit(nonNullHead)
        .map(CodingLiteral::encodeComponent).collect(Collectors.joining("|"));
  }

  @Nonnull
  private static String decodeComponent(@Nonnull final String component) {
    final Matcher matcher = NEEDS_UNQUOTING.matcher(component);
    if (matcher.matches()) {
      final String result = unSingleQuote(component);
      return unescapeFhirPathString(result);
    } else {
      return component;
    }
  }

  @Nonnull
  private static String encodeComponent(@Nullable final String component) {
    if (component == null) {
      return "";
    } else {
      final Matcher matcher = NEEDS_QUOTING.matcher(component);
      if (matcher.find()) {
        final String result = escapeFhirPathString(component);
        return "'" + result + "'";
      } else {
        return component;
      }
    }
  }

}
