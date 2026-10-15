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

package au.csiro.pathling.fhirpath.dsl;

import au.csiro.pathling.test.dsl.FhirPathDslTestBase;
import au.csiro.pathling.test.dsl.FhirPathTest;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.math.BigDecimal;
import java.util.stream.Stream;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Observation.ObservationComponentComponent;
import org.hl7.fhir.r4.model.Quantity;
import org.hl7.fhir.r4.model.Quantity.QuantityComparator;
import org.junit.jupiter.api.DynamicTest;

/**
 * Tests for the conversion functions applied to a FHIR Quantity. A FHIR Quantity maps to a FHIRPath
 * System.Quantity only if it has a value and a code, its system is UCUM (or the FHIRPath calendar
 * duration system), and it carries no comparator. Any other FHIR Quantity cannot be converted to a
 * Quantity or a String.
 *
 * @author John Grimes
 * @see <a href="https://hl7.org/fhir/R4/fhirpath.html#quantity">FHIRPath - Quantity</a>
 */
public class FhirQuantityConversionDslTest extends FhirPathDslTestBase {

  private static final String UCUM = "http://unitsofmeasure.org";
  private static final String CALENDAR = "https://hl7.org/fhirpath/N1/calendar-duration";
  private static final String LOCAL_UNITS = "http://example.org/units";

  @Nonnull
  private static Quantity quantity(
      @Nullable final String value,
      @Nullable final String unit,
      @Nullable final String system,
      @Nullable final String code) {
    final Quantity quantity = new Quantity().setUnit(unit).setSystem(system).setCode(code);
    if (value != null) {
      quantity.setValue(new BigDecimal(value));
    }
    return quantity;
  }

  @Nonnull
  private static ObservationComponentComponent component(@Nonnull final Quantity value) {
    return new ObservationComponentComponent().setValue(value);
  }

  /**
   * An Observation whose value is {@code 3 tab} with no system or code, with components holding a
   * representable quantity of each system alongside each way a quantity can fail to be
   * representable.
   */
  @Nonnull
  private static Observation observation() {
    final Observation observation = new Observation();
    observation.setId("fhir-quantities");
    observation.setStatus(Observation.ObservationStatus.FINAL);
    // value: 3 tab, with a unit but no system or code.
    observation.setValue(quantity("3", "tab", null, null));
    // component[0]: 5 mg in UCUM.
    observation.addComponent(component(quantity("5", "mg", UCUM, "mg")));
    // component[1]: 3 days in the FHIRPath calendar duration system.
    observation.addComponent(component(quantity("3", "days", CALENDAR, "day")));
    // component[2]: 10 widget in a non-UCUM system.
    observation.addComponent(component(quantity("10", "widget", LOCAL_UNITS, "widget")));
    // component[3]: 5 mg in UCUM, without a code.
    observation.addComponent(component(quantity("5", "mg", UCUM, null)));
    // component[4]: mg in UCUM, without a value.
    observation.addComponent(component(quantity(null, "mg", UCUM, "mg")));
    // component[5]: <5 mg in UCUM.
    final Quantity bounded = quantity("5", "mg", UCUM, "mg");
    bounded.setComparator(QuantityComparator.LESS_THAN);
    observation.addComponent(component(bounded));
    return observation;
  }

  @FhirPathTest
  public Stream<DynamicTest> testToString() {
    return builder()
        .withResource(observation())
        .group("Representable quantities")
        .testEquals(
            "5 'mg'", "component[0].value.ofType(Quantity).toString()", "UCUM quantity is quoted")
        .testEquals(
            "3 days",
            "component[1].value.ofType(Quantity).toString()",
            "calendar duration quantity is unquoted")
        .group("Non-representable quantities")
        .testEmpty("value.ofType(Quantity).toString()", "quantity without a system is empty")
        .testEmpty("component[2].value.ofType(Quantity).toString()", "non-UCUM quantity is empty")
        .testEmpty(
            "component[3].value.ofType(Quantity).toString()", "quantity without a code is empty")
        .testEmpty(
            "component[4].value.ofType(Quantity).toString()", "quantity without a value is empty")
        .testEmpty("component[5].value.ofType(Quantity).toString()", "bounded quantity is empty")
        .group("Array-backed input with one item")
        .testEquals(
            "3 days",
            "component.value.ofType(Quantity).where(code = 'day').toString()",
            "representable quantity is converted")
        .testEmpty(
            "component.value.ofType(Quantity).where(comparator.exists()).toString()",
            "bounded quantity is empty")
        .group("Multiple items")
        .testError("component.value.ofType(Quantity).toString()", "multiple quantities is an error")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testConvertsToString() {
    return builder()
        .withResource(observation())
        .group("Representable quantities")
        .testTrue(
            "component[0].value.ofType(Quantity).convertsToString()", "UCUM quantity converts")
        .testTrue(
            "component[1].value.ofType(Quantity).convertsToString()",
            "calendar duration quantity converts")
        .group("Non-representable quantities")
        .testFalse(
            "value.ofType(Quantity).convertsToString()",
            "quantity without a system does not convert")
        .testFalse(
            "component[2].value.ofType(Quantity).convertsToString()",
            "non-UCUM quantity does not convert")
        .testFalse(
            "component[3].value.ofType(Quantity).convertsToString()",
            "quantity without a code does not convert")
        .testFalse(
            "component[4].value.ofType(Quantity).convertsToString()",
            "quantity without a value does not convert")
        .testFalse(
            "component[5].value.ofType(Quantity).convertsToString()",
            "bounded quantity does not convert")
        .group("Array-backed input with one item")
        .testTrue(
            "component.value.ofType(Quantity).where(code = 'day').convertsToString()",
            "representable quantity converts")
        .testFalse(
            "component.value.ofType(Quantity).where(comparator.exists()).convertsToString()",
            "bounded quantity does not convert")
        .group("Multiple items")
        .testError(
            "component.value.ofType(Quantity).convertsToString()",
            "multiple quantities is an error")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testToQuantity() {
    return builder()
        .withResource(observation())
        .group("Representable quantities")
        .testTrue(
            "component[0].value.ofType(Quantity).toQuantity() = 5 'mg'",
            "UCUM quantity is returned unchanged")
        .testTrue(
            "component[1].value.ofType(Quantity).toQuantity() = 3 days",
            "calendar duration quantity is returned unchanged")
        .group("Non-representable quantities")
        .testEmpty("value.ofType(Quantity).toQuantity()", "quantity without a system is empty")
        .testEmpty("component[2].value.ofType(Quantity).toQuantity()", "non-UCUM quantity is empty")
        .testEmpty(
            "component[3].value.ofType(Quantity).toQuantity()", "quantity without a code is empty")
        .testEmpty(
            "component[4].value.ofType(Quantity).toQuantity()", "quantity without a value is empty")
        .testEmpty("component[5].value.ofType(Quantity).toQuantity()", "bounded quantity is empty")
        .group("With a unit")
        .testTrue(
            "component[0].value.ofType(Quantity).toQuantity('g') = 0.005 'g'",
            "UCUM quantity is converted to a compatible unit")
        .testEmpty(
            "component[5].value.ofType(Quantity).toQuantity('g')",
            "bounded quantity is not converted to a compatible unit")
        .testEmpty(
            "component[5].value.ofType(Quantity).toQuantity('mg')",
            "bounded quantity is not converted to its own unit")
        .testEmpty(
            "value.ofType(Quantity).toQuantity('tab')",
            "quantity without a system is not converted to its own unit")
        .group("Array-backed input with one item")
        .testTrue(
            "component.value.ofType(Quantity).where(code = 'day').toQuantity() = 3 days",
            "representable quantity is returned unchanged")
        .testEmpty(
            "component.value.ofType(Quantity).where(comparator.exists()).toQuantity()",
            "bounded quantity is empty")
        .group("Multiple items")
        .testError(
            "component.value.ofType(Quantity).toQuantity()", "multiple quantities is an error")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testConvertsToQuantity() {
    return builder()
        .withResource(observation())
        .group("Representable quantities")
        .testTrue(
            "component[0].value.ofType(Quantity).convertsToQuantity()", "UCUM quantity converts")
        .testTrue(
            "component[1].value.ofType(Quantity).convertsToQuantity()",
            "calendar duration quantity converts")
        .group("Non-representable quantities")
        .testFalse(
            "value.ofType(Quantity).convertsToQuantity()",
            "quantity without a system does not convert")
        .testFalse(
            "component[2].value.ofType(Quantity).convertsToQuantity()",
            "non-UCUM quantity does not convert")
        .testFalse(
            "component[3].value.ofType(Quantity).convertsToQuantity()",
            "quantity without a code does not convert")
        .testFalse(
            "component[4].value.ofType(Quantity).convertsToQuantity()",
            "quantity without a value does not convert")
        .testFalse(
            "component[5].value.ofType(Quantity).convertsToQuantity()",
            "bounded quantity does not convert")
        .group("With a unit")
        .testTrue(
            "component[0].value.ofType(Quantity).convertsToQuantity('g')",
            "UCUM quantity converts to a compatible unit")
        .testFalse(
            "component[5].value.ofType(Quantity).convertsToQuantity('g')",
            "bounded quantity does not convert to a compatible unit")
        .testFalse(
            "component[5].value.ofType(Quantity).convertsToQuantity('mg')",
            "bounded quantity does not convert to its own unit")
        .testFalse(
            "value.ofType(Quantity).convertsToQuantity('tab')",
            "quantity without a system does not convert to its own unit")
        .group("Array-backed input with one item")
        .testTrue(
            "component.value.ofType(Quantity).where(code = 'day').convertsToQuantity()",
            "representable quantity converts")
        .testFalse(
            "component.value.ofType(Quantity).where(comparator.exists()).convertsToQuantity()",
            "bounded quantity does not convert")
        .group("Multiple items")
        .testError(
            "component.value.ofType(Quantity).convertsToQuantity()",
            "multiple quantities is an error")
        .build();
  }
}
