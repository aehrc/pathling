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

package au.csiro.pathling.test.yaml;

import static au.csiro.pathling.test.yaml.YamlSupport.YAML;
import static au.csiro.pathling.test.yaml.YamlTestDefinition.TestCase.ANY_ERROR;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;

/**
 * A specification for FHIR Path test cases loaded from YAML configuration files.
 * <p>
 * This record represents a complete test specification that can contain multiple test cases for
 * evaluating FHIRPath expressions. The specification supports both simple expression tests and more
 * complex scenarios with custom subjects, variables, and error conditions.
 *
 * @param subject Optional FHIR resource or context object to use as the subject for test
 * expressions. This provides the base data against which FHIR Path expressions are evaluated.
 * @param cases List of individual test cases to execute. Each case contains an expression and
 * expected result or error condition.
 * @author Piotr Szul
 * @author John Grimes
 */
@SuppressWarnings("unchecked")
@Slf4j
public record YamlTestDefinition(
    @Nullable Map<Object, Object> subject,
    @Nonnull List<TestCase> cases
) {

  /**
   * Represents an individual test case within a FHIRPath test specification.
   * <p>
   * Each test case defines a FHIR Path expression to evaluate, along with the expected result or
   * error condition. Test cases can be configured with additional context such as custom input
   * files, models, and variables to support complex testing scenarios.
   * <p>
   * Test cases support both positive testing (expecting specific results) and negative testing
   * (expecting errors). Error tests can specify exact error messages or use the {@link #ANY_ERROR}
   * constant to accept any error condition.</p>
   *
   * @param description Optional human-readable description of what this test case validates. Used
   * for test reporting and debugging purposes.
   * @param expression The FHIR Path expression to evaluate. This is the core of the test case and
   * must be a valid FHIR Path syntax string.
   * @param errorMsg Expected error message when the test should fail. Use {@link #ANY_ERROR} to
   * accept any error, or null if the test should succeed.
   * @param result Expected result of the expression evaluation. Can be any valid JSON type (string,
   * number, boolean, array, object) or null for empty results.
   * @param inputFile Optional path to a file containing input data for the test. Useful for tests
   * that require large or complex FHIR resources.
   * @param model Optional model specification to use for the test. This can define specific FHIR
   * versions or profiles to validate against.
   * @param context Optional context string that provides additional evaluation context for the FHIR
   * Path expression.
   * @param disable Flag to temporarily disable this test case without removing it from the
   * specification. Disabled tests are skipped during execution.
   * @param variables Optional map of variables that can be referenced within the FHIR Path
   * expression using variable syntax (e.g., %variableName).
   * @param subject Optional test-specific subject to use instead of the suite-level subject. If
   * specified, this subject will be used for this test case, overriding the suite-level subject.
   */
  public record TestCase(
      @Nullable String description,
      @Nonnull String expression,
      @Nullable String errorMsg,
      @Nullable Object result,
      @Nullable String inputFile,
      @Nullable String model,
      @Nullable String context,
      boolean disable,
      @Nullable Map<String, Object> variables,
      @Nullable Map<Object, Object> subject
  ) {

    /**
     * Special constant used to indicate that any error message is acceptable for error test cases.
     * When {@link #errorMsg} is set to this value, the test will pass as long as the expression
     * evaluation throws any exception, regardless of the specific error message.
     */
    public static final String ANY_ERROR = "*";

    /**
     * Determines whether this test case is expecting an error condition.
     * <p>
     * A test case is considered an error test if it has a non-null {@link #errorMsg}. Error tests
     * are used to validate that invalid expressions or edge cases properly fail with appropriate
     * error messages.
     *
     * @return true if this test case expects an error, false if it expects a successful result
     */
    public boolean isError() {
      return nonNull(errorMsg);
    }

    /**
     * Determines whether this test case is a parse-only test.
     * <p>
     * A test case is considered parse-only if it has neither an expected error message nor an
     * expected result. Parse-only tests validate that expressions can be parsed and evaluated
     * without throwing exceptions, similar to bulk expression parsing tests. They are useful for
     * testing expression syntax validity without needing to verify specific outputs.
     *
     * @return true if this test case only validates parsing, false if it expects specific results
     * or errors
     */
    public boolean isExpressionOnly() {
      return errorMsg == null && result == null;
    }
  }

  /**
   * Maps a YAML case or group object into a stream of individual test cases.
   * <p>
   * This method handles the parsing of different YAML structures that can represent test cases. It
   * supports:
   * <ul>
   *   <li>Direct test case objects with an "expression" field</li>
   *   <li>Group objects containing lists of test cases</li>
   *   <li>Multiple expressions within a single test case definition</li>
   * </ul>
   * <p>
   * When a test case contains multiple expressions (as a list), each expression
   * is converted into a separate {@link TestCase} instance with the same configuration
   * but different expression values.
   *
   * @param caseOrGroup A map representing either a single test case or a group of test cases from
   * the YAML structure. Must not be null.
   * @return A stream of {@link TestCase} objects parsed from the input map. May be empty if the if
   * the input doesn't contain valid test case data.
   * @throws NullPointerException if caseOrGroup is null or contains null required fields
   */
  @Nonnull
  static Stream<TestCase> mapCaseOrGroup(@Nonnull final Map<Object, Object> caseOrGroup) {
    return mapCaseOrGroup(caseOrGroup, null);
  }

  /**
   * Maps a YAML case or group object into a stream of individual test cases.
   * <p>
   * This method handles the parsing of different YAML structures that can represent test cases. It
   * supports:
   * <ul>
   *   <li>Direct test case objects with an "expression" field</li>
   *   <li>Group objects containing lists of test cases</li>
   *   <li>Multiple expressions within a single test case definition</li>
   * </ul>
   * <p>
   * When a test case contains multiple expressions (as a list), each expression
   * is converted into a separate {@link TestCase} instance with the same configuration
   * but different expression values.
   *
   * @param caseOrGroup A map representing either a single test case or a group of test cases from
   * the YAML structure. Must not be null.
   * @param groupName Optional group name to prepend to test descriptions for better test
   * identification.
   * @return A stream of {@link TestCase} objects parsed from the input map. May be empty if the if
   * the input doesn't contain valid test case data.
   */
  @Nonnull
  static Stream<TestCase> mapCaseOrGroup(@Nonnull final Map<Object, Object> caseOrGroup,
      @Nullable final String groupName) {
    // Check if this is a direct test case (contains an "expression" field).
    if (caseOrGroup.containsKey("expression")) {
      // Extract expression(s) - could be a single string or list of strings.
      final List<String> expressions = toExpressions(requireNonNull(caseOrGroup.get("expression")));

      // Create a TestCase for each expression, sharing all other properties.
      return expressions.stream()
          .map(expr -> new TestCase(
              createDescription((String) caseOrGroup.get("desc"), groupName),
              // Test description with group prefix
              expr,                                       // Current expression from the list
              // Set error message to ANY_ERROR if "error" flag is true, otherwise null
              (boolean) caseOrGroup.computeIfAbsent("error", k -> false)
              ? ANY_ERROR
              : null,
              caseOrGroup.get("result"),                  // Expected result
              (String) caseOrGroup.get("inputfile"),      // Optional input file
              (String) caseOrGroup.get("model"),          // Optional model specification
              (String) caseOrGroup.get("context"),        // Optional context
              // Default "disable" to false if not present
              (boolean) caseOrGroup.computeIfAbsent("disable", k -> false),
              (Map<String, Object>) caseOrGroup.get("variables"), // Optional variables
              (Map<Object, Object>) caseOrGroup.get("subject")   // Optional per-test-case subject
          ));
    }
    // Check if this is a group object (single key-value pair where value is a list).
    else if (caseOrGroup.size() == 1) {
      final String currentGroupName = (String) caseOrGroup.keySet().iterator().next();
      final Object singleValue = caseOrGroup.values().iterator().next();
      // If the single value is a list, recursively process it as nested test cases with the group name.
      return singleValue instanceof final List<?> lst
             ? buildCases((List<Object>) lst, currentGroupName).stream()
             : Stream.empty(); // Not a list, so no valid test cases
    }
    // Object doesn't match expected structure for test cases or groups.
    else {
      return Stream.empty();
    }
  }

  /**
   * Converts various expression object types into a normalized list of expression strings.
   * <p>
   * YAML test specifications can define expressions in multiple formats:
   * <ul>
   *   <li>Single string expression: "Patient.name"</li>
   *   <li>List of expressions: ["Patient.name", "Patient.gender"]</li>
   * </ul>
   * This method normalizes these different formats into a consistent list of strings
   * that can be processed uniformly. If an unexpected object type is encountered,
   * it logs a warning and returns a failure expression for debugging purposes.
   *
   * @param expressionObj The expression object from the YAML, which can be a String or List. Must
   * not be null.
   * @return A list containing one or more expression strings. Never null or empty.
   * @throws NullPointerException if expressionObj is null
   */
  private static @Nonnull List<String> toExpressions(@Nonnull final Object expressionObj) {
    if (expressionObj instanceof String) {
      return List.of((String) expressionObj);
    } else if (expressionObj instanceof List<?>) {
      return (List<String>) expressionObj;
    } else {
      log.warn("Unexpected expression object: {}", expressionObj);
      return List.of("FAIL: " + expressionObj);
    }
  }

  /**
   * Creates a description by prepending the group name to the test description if both are
   * present.
   * <p>
   * This method helps with test identification in IDE test runners by including the group context
   * in the test description. The format is "groupName - description" when both are available.
   *
   * @param description The original test description from the YAML.
   * @param groupName The group name to prepend, or null if not in a group.
   * @return A formatted description string, or null if both inputs are null.
   */
  @Nullable
  private static String createDescription(@Nullable final String description,
      @Nullable final String groupName) {
    if (groupName != null && description != null) {
      return groupName + " - " + description;
    } else if (groupName != null) {
      return groupName;
    } else {
      return description;
    }
  }

  /**
   * Builds a list of test cases from a raw list of case objects parsed from YAML.
   * <p>
   * This method processes the "tests" section of a YAML test specification, converting each element
   * into one or more {@link TestCase} instances. It handles nested structures and groups by
   * delegating to {@link #mapCaseOrGroup(Map)} for each individual case.
   * <p>
   * The input list typically comes directly from YAML parsing and may contain:
   * <ul>
   *   <li>Individual test case maps</li>
   *   <li>Group objects containing multiple test cases</li>
   *   <li>Test cases with multiple expressions</li>
   * </ul>
   *
   * @param cases List of raw case objects from YAML parsing. Each object should be a Map
   * representing a test case or group. Must not be null.
   * @return A flattened list of {@link TestCase} objects ready for execution. Never null, but may
   * but may be empty if no valid test cases are found.
   * @throws ClassCastException if the cases list contains objects that cannot be cast to Map
   * @throws NullPointerException if cases is null
   */
  @Nonnull
  private static List<TestCase> buildCases(@Nonnull final Collection<Object> cases) {
    return cases.stream()
        .map(c -> (Map<Object, Object>) c)
        .flatMap(YamlTestDefinition::mapCaseOrGroup)
        .toList();
  }

  /**
   * Builds a list of test cases from a raw list of case objects parsed from YAML with group
   * context.
   * <p>
   * This overloaded method is used when processing test cases within a group. The group name is
   * passed down to be included in the test descriptions for better identification in test runners.
   *
   * @param cases List of raw case objects from YAML parsing. Each object should be a Map
   * representing a test case or group. Must not be null.
   * @param groupName The name of the group containing these test cases, used for test
   * identification.
   * @return A flattened list of {@link TestCase} objects ready for execution. Never null, but may
   * but may be empty if no valid test cases are found.
   * @throws ClassCastException if the cases list contains objects that cannot be cast to Map
   * @throws NullPointerException if cases is null
   */
  @Nonnull
  private static List<TestCase> buildCases(@Nonnull final Collection<Object> cases,
      @Nullable final String groupName) {
    return cases.stream()
        .map(c -> (Map<Object, Object>) c)
        .flatMap(caseOrGroup -> mapCaseOrGroup(caseOrGroup, groupName))
        .toList();
  }

  /**
   * Creates a YamlTestDefinition instance by parsing YAML data.
   * <p>
   * This is the primary factory method for creating test specifications from YAML files or strings.
   * The YAML structure should follow the expected format with optional "subject" and required
   * "tests" sections.
   *
   * @param yamlData String containing valid YAML data representing a test specification. Must not
   * be null and should contain at least a "tests" section.
   * @return A new instance with the parsed subject and test cases.
   * @throws IllegalArgumentException if the YAML is malformed or missing required sections
   * @throws NullPointerException if yamlData is null or the "tests" section is missing
   * @throws org.yaml.snakeyaml.parser.ParserException if the YAML syntax is invalid
   * @see YamlSupport#YAML
   */
  @Nonnull
  static YamlTestDefinition fromYaml(@Nonnull final String yamlData) {
    final Map<String, Object> yamlOM = YAML.load(yamlData);
    return new YamlTestDefinition(
        (Map<Object, Object>) yamlOM.get("subject"),
        buildCases((List<Object>) requireNonNull(yamlOM.get("tests")))
    );
  }
}
