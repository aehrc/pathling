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
 * Tests for comparisons involving a FHIR Quantity that carries a comparator. Such a quantity
 * records a bound rather than a known value (e.g. {@code <5 mg}), so any equality or comparison
 * involving it yields empty.
 *
 * @author John Grimes
 * @see <a href="https://github.com/aehrc/pathling/issues/2806">Quantity comparator is ignored in
 *     comparisons</a>
 */
public class BoundedQuantityDslTest extends FhirPathDslTestBase {

  private static final String UCUM = "http://unitsofmeasure.org";
  private static final String LOCAL_UNITS = "http://example.org/units";

  @Nonnull
  private static Quantity quantity(
      @Nullable final QuantityComparator comparator,
      @Nonnull final String value,
      @Nonnull final String system,
      @Nonnull final String code) {
    final Quantity quantity =
        new Quantity()
            .setValue(new BigDecimal(value))
            .setUnit(code)
            .setSystem(system)
            .setCode(code);
    if (comparator != null) {
      quantity.setComparator(comparator);
    }
    return quantity;
  }

  @Nonnull
  private static ObservationComponentComponent component(@Nonnull final Quantity value) {
    return new ObservationComponentComponent().setValue(value);
  }

  /**
   * An Observation whose value is {@code <5 mg}, with components holding a value of each other
   * comparator alongside unbounded controls.
   */
  @Nonnull
  private static Observation observation() {
    final Observation observation = new Observation();
    observation.setId("bounded-quantities");
    observation.setStatus(Observation.ObservationStatus.FINAL);
    observation.setValue(quantity(QuantityComparator.LESS_THAN, "5", UCUM, "mg"));
    // component[0]: 5 mg, unbounded.
    observation.addComponent(component(quantity(null, "5", UCUM, "mg")));
    // component[1]: <=5 mg.
    observation.addComponent(
        component(quantity(QuantityComparator.LESS_OR_EQUAL, "5", UCUM, "mg")));
    // component[2]: >5 mg.
    observation.addComponent(component(quantity(QuantityComparator.GREATER_THAN, "5", UCUM, "mg")));
    // component[3]: >=5 mg.
    observation.addComponent(
        component(quantity(QuantityComparator.GREATER_OR_EQUAL, "5", UCUM, "mg")));
    // component[4]: >=10 widget, in a non-UCUM system.
    observation.addComponent(
        component(quantity(QuantityComparator.GREATER_OR_EQUAL, "10", LOCAL_UNITS, "widget")));
    // component[5]: 10 widget, unbounded, in a non-UCUM system.
    observation.addComponent(component(quantity(null, "10", LOCAL_UNITS, "widget")));
    return observation;
  }

  @FhirPathTest
  public Stream<DynamicTest> testBoundedQuantityComparison() {
    return builder()
        .withResource(observation())
        .group("Bounded quantity compared with a literal")
        // <5 mg is below 5 mg, so treating it as exactly 5 mg would give a wrong answer.
        .testEmpty("value.ofType(Quantity) = 5 'mg'", "<5 mg = 5 mg is empty")
        .testEmpty("value.ofType(Quantity) != 5 'mg'", "<5 mg != 5 mg is empty")
        .testEmpty("value.ofType(Quantity) > 4 'mg'", "<5 mg > 4 mg is empty")
        .testEmpty("value.ofType(Quantity) < 5 'mg'", "<5 mg < 5 mg is empty")
        .testEmpty("value.ofType(Quantity) <= 5 'mg'", "<5 mg <= 5 mg is empty")
        .testEmpty("value.ofType(Quantity) >= 5 'mg'", "<5 mg >= 5 mg is empty")
        // The canonical (UCUM-normalised) comparison path is also suppressed.
        .testEmpty("value.ofType(Quantity) = 0.005 'g'", "<5 mg = 0.005 g is empty")
        .testEmpty("value.ofType(Quantity) < 1 'g'", "<5 mg < 1 g is empty")
        .group("Bounded quantity as the right operand")
        .testEmpty("5 'mg' = value.ofType(Quantity)", "5 mg = <5 mg is empty")
        .testEmpty("6 'mg' > value.ofType(Quantity)", "6 mg > <5 mg is empty")
        .testEmpty(
            "component[0].value.ofType(Quantity) = value.ofType(Quantity)",
            "unbounded 5 mg = <5 mg is empty")
        .group("Every comparator yields empty")
        .testEmpty("component[1].value.ofType(Quantity) = 5 'mg'", "<=5 mg = 5 mg is empty")
        .testEmpty("component[1].value.ofType(Quantity) <= 5 'mg'", "<=5 mg <= 5 mg is empty")
        .testEmpty("component[2].value.ofType(Quantity) > 4 'mg'", ">5 mg > 4 mg is empty")
        .testEmpty("component[2].value.ofType(Quantity) != 5 'mg'", ">5 mg != 5 mg is empty")
        .testEmpty("component[3].value.ofType(Quantity) = 5 'mg'", ">=5 mg = 5 mg is empty")
        .testEmpty("component[3].value.ofType(Quantity) < 6 'mg'", ">=5 mg < 6 mg is empty")
        .group("Bounded quantity compared with itself")
        .testEmpty("value.ofType(Quantity) = value.ofType(Quantity)", "<5 mg = <5 mg is empty")
        .testEmpty("value.ofType(Quantity) <= value.ofType(Quantity)", "<5 mg <= <5 mg is empty")
        .group("Unbounded quantities are unaffected")
        .testTrue("component[0].value.ofType(Quantity) = 5 'mg'", "5 mg = 5 mg is true")
        .testTrue("component[0].value.ofType(Quantity) > 4 'mg'", "5 mg > 4 mg is true")
        .testFalse("component[0].value.ofType(Quantity) < 5 'mg'", "5 mg < 5 mg is false")
        .testTrue("component[0].value.ofType(Quantity) = 0.005 'g'", "5 mg = 0.005 g is true")
        .group("Bounded quantity in a non-UCUM system")
        .testTrue(
            "component[5].value.ofType(Quantity) = component[5].value.ofType(Quantity)",
            "unbounded 10 widget = 10 widget is true")
        .testEmpty(
            "component[4].value.ofType(Quantity) = component[5].value.ofType(Quantity)",
            ">=10 widget = 10 widget is empty")
        .testEmpty(
            "component[4].value.ofType(Quantity) >= component[5].value.ofType(Quantity)",
            ">=10 widget >= 10 widget is empty")
        .testEmpty(
            "component[5].value.ofType(Quantity) < component[4].value.ofType(Quantity)",
            "10 widget < >=10 widget is empty")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testBoundedQuantityInCollectionOperations() {
    return builder()
        .withResource(observation())
        .group("Deduplication")
        // A bounded quantity is never equal to anything, so it is never deduplicated.
        .testEquals(
            2,
            "(value.ofType(Quantity) | value.ofType(Quantity)).count()",
            "union keeps both copies of a bounded quantity")
        .testEquals(
            1,
            "(component[0].value.ofType(Quantity) | component[0].value.ofType(Quantity)).count()",
            "union deduplicates an unbounded quantity")
        .testEquals(
            2,
            "value.ofType(Quantity).combine(value.ofType(Quantity)).distinct().count()",
            "distinct() keeps both copies of a bounded quantity")
        .testTrue(
            "value.ofType(Quantity).combine(component[0].value.ofType(Quantity)).isDistinct()",
            "isDistinct() is true for a bounded quantity and its bound")
        .group("Membership")
        .testFalse(
            "value.ofType(Quantity) in (value.ofType(Quantity) |"
                + " component[0].value.ofType(Quantity))",
            "a bounded quantity is not found in a collection, not even itself")
        .testFalse(
            "component.value.ofType(Quantity) contains component[1].value.ofType(Quantity)",
            "contains does not find a bounded quantity")
        .testTrue(
            "component[0].value.ofType(Quantity) in component.value.ofType(Quantity)",
            "an unbounded quantity is found among bounded ones")
        .build();
  }
}
