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
import java.util.List;
import java.util.stream.Stream;
import org.hl7.fhir.r4.model.Enumerations.AdministrativeGender;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Quantity;
import org.junit.jupiter.api.DynamicTest;

/**
 * Tests for the FHIRPath indexer operator ({@code [index]}).
 *
 * @author John Grimes
 * @see <a href="https://hl7.org/fhirpath/#index-integer-collection">Indexer operation</a>
 */
public class IndexerOperatorDslTest extends FhirPathDslTestBase {

  @FhirPathTest
  public Stream<DynamicTest> testIndexerOnArrays() {
    return builder()
        .withSubject(
            sb ->
                sb.stringArray("stringArray", "one", "two", "three")
                    .stringEmpty("emptyString")
                    .elementArray(
                        "people",
                        person1 -> person1.string("name", "Alice"),
                        person2 -> person2.string("name", "Bob")))
        .group("Indexes within bounds")
        .testEquals("one", "stringArray[0]", "Index 0 returns the first item")
        .testEquals("three", "stringArray[2]", "Last index returns the last item")
        .testEquals("Bob", "people[1].name", "Index into a complex array")
        .group("Indexes outside bounds")
        .testEmpty("stringArray[3]", "Index equal to the size is empty")
        .testEmpty("stringArray[100]", "Index far beyond the size is empty")
        .testEmpty("stringArray[-1]", "Index -1 is empty rather than an error")
        .testEmpty("stringArray[-2]", "Negative index does not count from the end")
        .testEmpty("stringArray[-3]", "Negative index matching the size is empty")
        .testEmpty("stringArray[-4]", "Negative index beyond the size is empty")
        .testEmpty("people[-1].name", "Negative index into a complex array is empty")
        .group("Empty subject or index")
        .testEmpty("emptyString[0]", "Indexing an empty collection is empty")
        .testEmpty("{}[0]", "Indexing an empty literal is empty")
        .testEmpty("stringArray[{}]", "An empty index is empty")
        .group("Computed indexes")
        .testEquals(
            "three",
            "stringArray[stringArray.count() - 1]",
            "Index computed from count() returns the last item")
        .testEquals("two", "stringArray[1 + 0]", "Index computed by addition")
        .testEquals("two", "stringArray[people.count() - 1]", "Index computed from another path")
        .testEmpty(
            "stringArray[stringArray.count()]", "Index computed from count() beyond the bounds")
        .testEmpty(
            "stringArray[0 - stringArray.count()]", "Negative index computed from count() is empty")
        .testEmpty("stringArray[2147483647]", "The largest integer index is empty")
        .testEmpty(
            "stringArray[2147483647 + 1]", "An index computed beyond the integer range is empty")
        .group("Invalid indexes")
        .testError(
            "Indexer operator requires an Integer index",
            "stringArray['a']",
            "A string index is an error")
        .testError(
            "Indexer operator requires an Integer index",
            "stringArray[1.5]",
            "A decimal index is an error")
        .testError(
            "Indexer operator requires a singular index",
            "stringArray[(0 | 1)]",
            "An index with more than one item is an error")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testIndexerOnSingularElements() {
    return builder()
        .withSubject(
            sb ->
                sb.string("singleString", "test")
                    .integer("singleInteger", 42)
                    .element("person", person -> person.string("name", "John")))
        .group("Singular primitive elements")
        .testEquals("test", "singleString[0]", "Index 0 returns the singular element")
        .testEmpty("singleString[1]", "Index 1 of a singular element is empty")
        .testEmpty("singleString[-1]", "Index -1 of a singular element is empty")
        .testEquals(42, "singleInteger[0]", "Index 0 returns the singular integer")
        .testEmpty("singleInteger[1]", "Index 1 of a singular integer is empty")
        .group("Singular complex elements")
        .testEquals("John", "person[0].name", "Index 0 returns the singular complex element")
        .testEmpty("person[1].name", "Index 1 of a singular complex element is empty")
        .group("Literals")
        .testEquals("a", "'a'[0]", "Index 0 of a literal returns it")
        .testEmpty("'a'[1]", "Index 1 of a literal is empty")
        .testEquals("a", "'a'[1 - 1]", "Computed index 0 of a literal returns it")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testIndexerOnPatient() {
    final Patient patient = new Patient();
    patient.setGender(AdministrativeGender.MALE);
    patient.addName().setFamily("First").addGiven("A").addGiven("B");
    patient.addName().setFamily("Second").addGiven("C");
    patient.addName().setFamily("Third").addGiven("D").addGiven("E");

    return builder()
        .withResource(patient)
        .group("Repeating element")
        .testEquals("Second", "name[1].family", "Index 1 returns the second name")
        .testEmpty("name[-1]", "Index -1 is empty rather than an error")
        .testEmpty("name[-2]", "Index -2 is empty rather than the second last name")
        .testEquals(
            "Third", "name[name.count() - 1].family", "Index computed from count() is the last")
        .group("Singular element")
        .testEquals("male", "gender[0]", "Index 0 returns the singular element")
        .testEmpty("gender[1]", "Index 1 of a singular element is empty")
        .testEmpty("gender[-1]", "Index -1 of a singular element is empty")
        .group("Indexing within select()")
        .testEquals(
            List.of("B", "E"),
            "name.select(given[1])",
            "Index 1 of each name's given names skips names with only one")
        .testEquals(
            List.of("A", "C", "D"), "name.select(given[0])", "Index 0 of each name's given names")
        .testEmpty("name.select(family[1])", "Index 1 of each name's singular family is empty")
        .testEmpty("name.select(given[-1])", "Index -1 within select() is empty")
        .group("Resource")
        .testEquals("male", "Patient[0].gender", "Index 0 returns the resource")
        .testEmpty("Patient[1].gender", "Index 1 of the resource is empty")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testIndexerOnChoiceElement() {
    final Observation observation = new Observation();
    observation.setValue(new Quantity().setValue(5).setUnit("mg"));

    return builder()
        .withResource(observation)
        .group("Singular choice element")
        .testEquals(
            5, "value.ofType(Quantity)[0].value", "Index 0 returns the singular choice element")
        .testEmpty("value.ofType(Quantity)[1]", "Index 1 of a singular choice element is empty")
        .build();
  }
}
