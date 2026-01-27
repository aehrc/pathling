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

package au.csiro.pathling.fhirpath.dsl;

import static au.csiro.pathling.test.yaml.FhirTypedLiteral.toQuantity;

import au.csiro.pathling.test.dsl.FhirPathDslTestBase;
import au.csiro.pathling.test.dsl.FhirPathTest;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;

/** Tests for FHIRPath 'as' type-casting operator. */
public class AsOperatorDslTest extends FhirPathDslTestBase {

  @FhirPathTest
  public Stream<DynamicTest> testAsOperator() {
    return builder()
        .withSubject(
            sb ->
                sb
                    // Primitive types for as operator testing
                    .string("stringValue", "test")
                    .integer("integerValue", 42)
                    .decimal("decimalValue", 3.14)
                    .bool("booleanValue", true)
                    .stringEmpty("emptyString")
                    .stringArray("stringArray", "one", "two", "three")
                    .coding("codingValue", "http://example.org/codesystem|code2|display1")
                    .quantity("quantityValue", "11.5 'mg'")
                    // Heterogeneous collection
                    .elementArray(
                        "heteroattr",
                        val1 ->
                            val1.choice("value")
                                .string("valueString", "string")
                                .integerEmpty("valueInteger")
                                .boolEmpty("valueBoolean"),
                        val2 -> val2.integer("valueInteger", 1),
                        val3 -> val3.integer("valueInteger", 2)))
        .group("'as' operator - primitive type matching (positive cases)")
        // Positive type matches - should return the actual value
        .testEquals(
            "test",
            "stringValue as System.String",
            "'as' operator returns value when it matches System.String type")
        .testEquals(
            42,
            "integerValue as Integer",
            "'as' operator returns value when it matches Integer type")
        .testEquals(
            3.14,
            "decimalValue as decimal",
            "'as' operator returns value when it matches decimal type")
        .testTrue(
            "booleanValue as FHIR.boolean",
            "'as' operator returns value when it matches FHIR.boolean type")
        .group("'as' operator - type mismatches (negative cases)")
        // Negative type matches - should return empty collection
        .testEmpty("stringValue as Integer", "'as' operator returns empty when type doesn't match")
        .testEmpty(
            "integerValue as Boolean", "'as' operator returns empty when value is different type")
        .testEmpty(
            "codingValue as Quantity",
            "'as' operator returns empty when complex type doesn't match")
        .group("'as' operator - complex types")
        // Complex type matching with property traversal
        .testEquals(
            11.5,
            "((11.5 'mg') as Quantity).value",
            "'as' operator returns Quantity value and allows traversal")
        .testEquals(
            "mg",
            "((11.5 'mg') as FHIR.Quantity).unit",
            "'as' operator works with explicit FHIR namespace")
        .testEquals(
            "code2",
            "(codingValue as FHIR.Coding).code",
            "'as' operator returns Coding value and allows traversal")
        .testEquals(
            "code2",
            "(codingValue as System.Coding).code",
            "'as' operator works with System namespace")
        .group("'as' operator - namespace variations")
        // Test namespace handling
        .testEquals(
            toQuantity("11 'mg'"),
            "(11 'mg') as Quantity",
            "'as' operator works with unqualified type name")
        .testEquals(
            toQuantity("12 'cm'"),
            "(12 'cm') as System.Quantity",
            "'as' operator works with System namespace for Quantity")
        .testEquals(
            toQuantity("13 'mg'"),
            "(13 'mg') as FHIR.Quantity",
            "'as' operator works for System.Quantity with FHIR namespace")
        .group("'as' operator - edge cases")
        // Empty collections
        .testEmpty(
            "emptyString as String", "'as' operator returns empty when applied to empty value")
        .testEmpty("{} as String", "'as' operator returns empty when applied to empty collection")

        // Multi-item collections should error
        .testError("stringArray as String", "'as' operator throws error on multi-item collection")
        .group("'as' operator - with choice elements")
        .testEquals(
            "string",
            "heteroattr.first().value as String",
            "'as' operator extracts String from choice element")
        .testEmpty(
            "heteroattr.first().value as Integer",
            "'as' operator returns empty when type doesn't match in choice element")
        .testEquals(
            1,
            "heteroattr[1].value as Integer",
            "'as' operator extracts Integer from choice element")
        .group("'as' operator vs as() function equivalence")
        // Show that operator and function syntax produce identical results
        .testEquals(
            "test",
            "stringValue as String",
            "'as' operator returns value for matching type (like function)")
        .testEmpty(
            "stringValue as Integer",
            "'as' operator returns empty for non-matching type (like function)")
        .testEmpty(
            "emptyString as String",
            "'as' operator returns empty for empty collection (like function)")
        .testEquals(
            11.0,
            "((11 'mg') as Quantity).value",
            "'as' operator allows property access after cast (like function)")
        .group("'as' operator - integration with other expressions")
        // Integration with other FHIRPath operations
        .testTrue(
            "(stringValue as String).exists()", "'as' operator result can be tested with exists()")
        .testFalse(
            "(stringValue as Integer).exists()",
            "'as' operator empty result returns false for exists()")
        .group("'as' operator - comparison with 'is' operator")
        // Show relationship between 'is' and 'as' operators
        .testTrue(
            "stringValue is String and (stringValue as String).exists()",
            "'is' returns true corresponds to 'as' returning a value")
        .testTrue(
            "(stringValue is Integer).not() and (stringValue as Integer).empty()",
            "'is' returns false corresponds to 'as' returning empty")
        .build();
  }
}
