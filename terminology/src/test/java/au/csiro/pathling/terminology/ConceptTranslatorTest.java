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

import static org.junit.jupiter.api.Assertions.assertEquals;

import au.csiro.pathling.fhirpath.encoding.ImmutableCoding;
import au.csiro.pathling.fhirpath.encoding.SimpleCoding;
import au.csiro.pathling.test.fixtures.ConceptTranslatorBuilder;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class ConceptTranslatorTest {

  static final SimpleCoding SIMPLE_CODING_1 = new SimpleCoding("uuid:system1", "code1");
  static final SimpleCoding SIMPLE_CODING_2 = new SimpleCoding("uuid:system1", "code2");
  static final SimpleCoding SIMPLE_CODING_3 = new SimpleCoding("uuid:system2", "code2",
      "12");

  @Test
  void testEmptyTranslation() {
    final ConceptTranslator emptyConceptTranslator = ConceptTranslatorBuilder.empty().build();
    assertEquals(Collections.emptyList(),
        emptyConceptTranslator.translate(null));
    assertEquals(Collections.emptyList(),
        emptyConceptTranslator.translate(Collections.emptyList()));
    assertEquals(Collections.emptyList(),
        emptyConceptTranslator
            .translate(Arrays.asList(SIMPLE_CODING_1, SIMPLE_CODING_2, SIMPLE_CODING_3)));
  }

  @Test
  void testNonEmptyTranslations() {
    final ConceptTranslator testConceptTranslator = ConceptTranslatorBuilder
        .toSystem("uuid:system-dest")
        .putTimes(SIMPLE_CODING_1, 2)
        .build();

    assertEquals(Collections.emptyList(),
        testConceptTranslator.translate(null));

    assertEquals(Collections.emptyList(),
        testConceptTranslator.translate(Collections.emptyList()));

    assertEquals(Collections.emptyList(),
        testConceptTranslator
            .translate(Arrays.asList(SIMPLE_CODING_2, SIMPLE_CODING_3, SIMPLE_CODING_2)));

    assertEquals(
        Arrays.asList(ImmutableCoding.of("uuid:system-dest", "code1-0", "Display-0"),
            ImmutableCoding.of("uuid:system-dest", "code1-1", "Display-1")),
        testConceptTranslator
            .translate(Arrays.asList(SIMPLE_CODING_1, SIMPLE_CODING_3, SIMPLE_CODING_1)).stream()
            .map(
                ImmutableCoding::of).collect(Collectors.toList()));
  }
}
