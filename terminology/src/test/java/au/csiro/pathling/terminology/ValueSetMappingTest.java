/*
 * Copyright 2022 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.terminology;


import static au.csiro.pathling.test.TestResources.getResourceAsStream;
import static org.junit.jupiter.api.Assertions.assertEquals;

import au.csiro.pathling.fhirpath.encoding.SimpleCoding;
import ca.uhn.fhir.context.FhirContext;
import com.google.common.collect.ImmutableSet;
import java.util.Collections;
import java.util.Set;
import org.hl7.fhir.r4.model.ValueSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ValueSetMappingTest extends MappingTest {

  FhirContext fhirContext;

  static final String MY_VALUE_SET_URL = "https://csiro.au/fhir/ValueSet/my-value-set";

  static final String SYSTEM_1 = "uuid:system1";
  static final String SYSTEM_2 = "uuid:system2";

  @BeforeEach
  void setUp() {
    fhirContext = FhirContext.forR4();
  }

  @Test
  void toIntersectionEmpty() {
    final ValueSet valueSet = ValueSetMapping
        .toIntersection(MY_VALUE_SET_URL, Collections.emptySet());
    assertRequest(valueSet);
  }

  @Test
  void toIntersectionVersioned() {
    final ValueSet valueSet = ValueSetMapping
        .toIntersection(MY_VALUE_SET_URL,
            ImmutableSet.of(
                new SimpleCoding(SYSTEM_1, "code1"),
                new SimpleCoding(SYSTEM_1, "code1", "v1"),
                new SimpleCoding(SYSTEM_1, "code1", "v2"),
                new SimpleCoding(SYSTEM_1, "code2"),
                new SimpleCoding(SYSTEM_1, "code2", "v1"),
                new SimpleCoding(SYSTEM_1, "code2", "v2"),
                new SimpleCoding(SYSTEM_1, "codeA"),
                new SimpleCoding(SYSTEM_1, "codeB", "v1"),
                new SimpleCoding(SYSTEM_1, "codeC", "v2")
            ));
    assertRequest(valueSet);
  }


  @Test
  void toIntersectionManySystems() {
    final ValueSet valueSet = ValueSetMapping
        .toIntersection(MY_VALUE_SET_URL,
            ImmutableSet.of(
                new SimpleCoding(SYSTEM_1, "code1"),
                new SimpleCoding(SYSTEM_1, "code1", "v1"),
                new SimpleCoding(SYSTEM_1, "codeA"),
                new SimpleCoding(SYSTEM_1, "codeA", "v1"),
                new SimpleCoding(SYSTEM_2, "code1"),
                new SimpleCoding(SYSTEM_2, "code1", "v1"),
                new SimpleCoding(SYSTEM_2, "codeB"),
                new SimpleCoding(SYSTEM_2, "codeB", "v1")
            ));
    assertRequest(valueSet);
  }


  @Test
  void toIntersectionUndefined() {
    final ValueSet valueSet = ValueSetMapping
        .toIntersection(MY_VALUE_SET_URL,
            ImmutableSet.of(
                new SimpleCoding(null, null),
                new SimpleCoding(null, "code1", "v1"),
                new SimpleCoding(SYSTEM_1, null)
            ));
    assertRequest(valueSet, "toIntersectionEmpty");
  }


  @Test
  void codingSetFromNullExpansion() {

    final Set<SimpleCoding> actualCodingSet = ValueSetMapping
        .codingSetFromExpansion(null);
    assertEquals(Collections.emptySet(), actualCodingSet);
  }


  @Test
  void codingSetFromEmptyExpansion() {

    final Set<SimpleCoding> actualCodingSet = ValueSetMapping
        .codingSetFromExpansion(new ValueSet());
    assertEquals(Collections.emptySet(), actualCodingSet);
  }


  @Test
  void codingSetFromExpansion() {

    final ValueSet expansionValueSet = (ValueSet) jsonParser.parseResource(
        getResourceAsStream("txResponses/ValueSetMappingTest/twoCoding.ValueSet.json"));

    final Set<SimpleCoding> actualCodingSet = ValueSetMapping
        .codingSetFromExpansion(expansionValueSet);

    assertEquals(ImmutableSet.of(new SimpleCoding(SYSTEM_1, "code1"),
        new SimpleCoding(SYSTEM_2, "code2", "v1")), actualCodingSet);

  }
}
