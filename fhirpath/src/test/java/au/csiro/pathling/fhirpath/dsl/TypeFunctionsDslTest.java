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

import au.csiro.pathling.test.dsl.FhirPathDslTestBase;
import au.csiro.pathling.test.dsl.FhirPathTest;
import java.util.stream.Stream;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.jupiter.api.DynamicTest;

/**
 * Tests for FHIRPath type functions.
 */
public class TypeFunctionsDslTest extends FhirPathDslTestBase {

  @FhirPathTest
  public Stream<DynamicTest> testIsFunction() {
    return builder()
        .withSubject(sb -> sb
            // Primitive types for is() testing
            .string("stringValue", "test")
            .integer("integerValue", 42)
            .decimal("decimalValue", 3.14)
            .bool("booleanValue", true)
            .stringEmpty("emptyString")
            .stringArray("stringArray", "one", "two", "three")
            .coding("codingValue", "http://example.org/codesystem|code2|display1")
            .element("quantityValue",
                qt -> qt.fhirType(FHIRDefinedType.QUANTITY)
                    .decimal("value", 11.5)
                    .string("unit", "mg"))
            // Heterogeneous collection
            .elementArray("heteroattr",
                val1 -> val1.choice("value")
                    .string("valueString", "string")
                    .integerEmpty("valueInteger")
                    .boolEmpty("valueBoolean"),
                val2 -> val2.integer("valueInteger", 1),
                val3 -> val3.integer("valueInteger", 2)
            )
        )
        .group("is() function - primitive type matching")
        // Positive type matches
        .testTrue("stringValue.is(System.String)",
            "is() returns true when value matches System.String type")
        .testTrue("integerValue.is(Integer)",
            "is() returns true when value matches Integer type")
        .testTrue("decimalValue.is(decimal)",
            "is() returns true when value matches decimal type")
        .testTrue("booleanValue.is(FHIR.boolean)",
            "is() returns true when value matches FHIR.boolean type")

        .group("is() function - type mismatches")
        // Negative type matches
        .testFalse("stringValue.is(Integer)",
            "is() returns false when type doesn't match")
        .testFalse("integerValue.is(Boolean)",
            "is() returns false when value is different type")
        .testFalse("codingValue.is(Quantity)",
            "is() returns false when complex type doesn't match")

        .group("is() function - complex types")
        // Complex type matching
        .testTrue("quantityValue.is(Quantity)",
            "is() returns true for Quantity complex type")
        .testTrue("quantityValue.is(FHIR.Quantity)",
            "is() returns true with explicit FHIR namespace")
        .testTrue("codingValue.is(FHIR.Coding)",
            "is() returns true for Coding with FHIR namespace")
        .testTrue("codingValue.is(System.Coding)",
            "is() returns true for Coding with System namespace")
        .testTrue("codingValue.is(Coding)",
            "is() returns true for Coding with unqualified name")

        .group("is() function - edge cases")
        // Empty collections
        .testEmpty("emptyString.is(String)",
            "is() returns empty when applied to empty value")
        .testEmpty("{}.is(String)",
            "is() returns empty when applied to empty collection")

        // Multi-item collections should error
        .testError("stringArray.is(String)",
            "is() throws error on multi-item collection")

        .group("is() function - integration with other functions")
        // Integration with boolean functions
        .testFalse("stringValue.is(String).not()",
            "is() result can be negated with not()")
        .testTrue("stringValue.is(Integer).not()",
            "is() false result can be negated with not()")

        .build();
  }

}
