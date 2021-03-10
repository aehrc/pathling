/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.terminology;

import static org.junit.jupiter.api.Assertions.assertEquals;

import au.csiro.pathling.fhirpath.encoding.ImmutableCoding;
import au.csiro.pathling.fhirpath.encoding.SimpleCoding;
import au.csiro.pathling.test.fixtures.ConceptTranslatorBuilder;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
@Tag("UnitTest")
public class ConceptTranslatorTest {


  private static final SimpleCoding SIMPLE_CODING_1 = new SimpleCoding("uuid:system1", "code1");
  private static final SimpleCoding SIMPLE_CODING_2 = new SimpleCoding("uuid:system1", "code2");
  private static final SimpleCoding SIMPLE_CODING_3 = new SimpleCoding("uuid:system2", "code2",
      "12");

  @Test
  public void testEmptyTranslation() {
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
  public void testNonEmptyTranslations() {
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

    assertEquals(Arrays.asList(ImmutableCoding.of("uuid:system-dest", "code1-0", "Display-0"),
        ImmutableCoding.of("uuid:system-dest", "code1-1", "Display-1")),
        testConceptTranslator
            .translate(Arrays.asList(SIMPLE_CODING_1, SIMPLE_CODING_3, SIMPLE_CODING_1)).stream()
            .map(
                ImmutableCoding::of).collect(Collectors.toList()));
  }
}
