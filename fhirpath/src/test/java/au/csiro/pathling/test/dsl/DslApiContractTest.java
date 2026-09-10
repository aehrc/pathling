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

package au.csiro.pathling.test.dsl;

import static au.csiro.pathling.test.dsl.TypeInfoExpectation.toTypeInfo;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Patient;
import org.junit.jupiter.api.DynamicTest;

/**
 * Exercises every construct of the FHIRPath test DSL in one place: each model builder method, each
 * assertion overload, both subject kinds, and the {@link TypeInfoExpectation} helper.
 *
 * <p>This is a contract test rather than a behavioural one. The DSL surface is documented for
 * authors — including in the {@code fhirpath-test-designer} skill — and that documentation goes
 * stale silently when the DSL changes. Anything removed or renamed here fails the build instead, so
 * the documented surface and the real one cannot drift apart unnoticed.
 *
 * <p>When adding a method to {@link FhirPathModelBuilder} or {@link FhirPathTestBuilder}, add it
 * here too.
 *
 * <p>One method is deliberately not exercised: {@link FhirPathTestBuilder#test(String)} builds a
 * case with no expression and no expectation, so it cannot be run. It is undocumented for that
 * reason, and is the only public builder method this test does not cover.
 */
public class DslApiContractTest extends FhirPathDslTestBase {

  @FhirPathTest
  public Stream<DynamicTest> probeAssertionsAndBuilders() {
    return builder()
        .withSubject(
            sb ->
                sb.string("singleString", "test")
                    .stringEmpty("emptyString")
                    .stringArray("stringArray", "one", "two")
                    .integer("singleInteger", 42)
                    .integerEmpty("emptyInteger")
                    .integerArray("integerArray", 1, 2)
                    .decimal("singleDecimal", 1.5)
                    .decimalEmpty("emptyDecimal")
                    .decimalArray("decimalArray", 1.5, 2.5)
                    .bool("singleBool", true)
                    .boolEmpty("emptyBool")
                    .boolArray("boolArray", true, false)
                    .date("singleDate", "2024-01-15")
                    .dateEmpty("emptyDate")
                    .dateArray("dateArray", "2024-01-15", "2024-02-15")
                    .dateTime("singleDateTime", "2024-01-15T10:30:00Z")
                    .dateTimeEmpty("emptyDateTime")
                    .dateTimeArray("dateTimeArray", "2024-01-15T10:30:00Z")
                    .time("singleTime", "10:30:00")
                    .timeEmpty("emptyTime")
                    .timeArray("timeArray", "10:30:00")
                    .coding("singleCoding", "http://loinc.org|1234-5")
                    .codingEmpty("emptyCoding")
                    .codingArray("codingArray", "http://loinc.org|1234-5")
                    .quantity("singleQuantity", "10.5 'mg'")
                    .quantityEmpty("emptyQuantity")
                    .quantityArray("quantityArray", "10.5 'mg'", "20 'kg'")
                    .element("person", p -> p.string("name", "John"))
                    .elementEmpty("emptyElement")
                    .elementArray(
                        "people", p1 -> p1.string("name", "Alice"), p2 -> p2.string("name", "Bob"))
                    .element("ref", r -> r.fhirReference().string("reference", "Patient/1"))
                    .element("typed", t -> t.fhirType(FHIRDefinedType.QUANTITY))
                    .element("chosen", c -> c.choice("value")))
        .group("assertion surface")
        .testEquals("test", "singleString", "testEquals with a scalar expectation")
        .testEquals(
            List.of("one", "two"), "stringArray", "testEquals with a collection expectation")
        .testTrue("singleBool", "testTrue")
        .testFalse("singleBool.not()", "testFalse")
        .testEmpty("emptyString", "testEmpty")
        .testError("singleString + 1", "testError with any error")
        .testError(
            "Math operator (+) requires the left operand to be singular.",
            "integerArray + 1",
            "testError with a specific message")
        .group("low-level escape hatch")
        .test("test() with an explicit case builder", tc -> tc.expression("1 + 1").expectResult(2))
        .test("test() expecting any error", tc -> tc.expression("'a' + 1").expectError())
        .group("type info expectations")
        .testEquals(toTypeInfo("System.Integer(System.Any)"), "(1).type()", "toTypeInfo")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> probeMapSubject() {
    final Map<String, Object> model =
        new FhirPathModelBuilder().string("preBuiltString", "value").build();

    return builder()
        .withSubject(model)
        .group("withSubject(Map)")
        .testEquals(
            "value", "preBuiltString", "Pre-built model map, via FhirPathModelBuilder.build()")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> probeResourceSubject() {
    final Patient patient = new Patient();
    patient.setId("patient-1");
    patient.setActive(true);

    return builder()
        .withResource(patient)
        .group("withResource")
        .testTrue("Patient.active", "Resource-prefixed path against a real HAPI resource")
        .build();
  }
}
