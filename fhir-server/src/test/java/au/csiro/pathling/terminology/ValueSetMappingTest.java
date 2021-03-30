/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.terminology;


import static au.csiro.pathling.test.helpers.TestHelpers.getResourceAsStream;
import static org.junit.jupiter.api.Assertions.assertEquals;

import au.csiro.pathling.fhirpath.encoding.SimpleCoding;
import ca.uhn.fhir.context.FhirContext;
import com.google.common.collect.ImmutableSet;
import java.util.Collections;
import java.util.Set;
import org.hl7.fhir.r4.model.ValueSet;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

@Tag("UnitTest")
public class ValueSetMappingTest extends MappingTest {

  @Autowired
  protected FhirContext fhirContext;

  private static final String MY_VALUE_SET_URL = "https://csiro.au/fhir/ValueSet/my-value-set";

  private static final String SYSTEM_1 = "uuid:system1";
  private static final String SYSTEM_2 = "uuid:system2";

  @Test
  public void toIntersectionEmpty() {
    final ValueSet valueSet = ValueSetMapping
        .toIntersection(MY_VALUE_SET_URL, Collections.emptySet());
    assertRequest(valueSet);
  }

  @Test
  public void toIntersectionVersioned() {
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
  public void toIntersectionManySystems() {
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
  public void toIntersectionUndefined() {
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
  public void codingSetFromNullExpansion() {

    final Set<SimpleCoding> actualCodingSet = ValueSetMapping
        .codingSetFromExpansion(null);
    assertEquals(Collections.emptySet(), actualCodingSet);
  }


  @Test
  public void codingSetFromEmptyExpansion() {

    final Set<SimpleCoding> actualCodingSet = ValueSetMapping
        .codingSetFromExpansion(new ValueSet());
    assertEquals(Collections.emptySet(), actualCodingSet);
  }


  @Test
  public void codingSetFromExpansion() {

    final ValueSet expansionValueSet = (ValueSet) jsonParser.parseResource(
        getResourceAsStream("txResponses/ValueSetMappingTest/twoCoding.ValueSet.json"));

    final Set<SimpleCoding> actualCodingSet = ValueSetMapping
        .codingSetFromExpansion(expansionValueSet);

    assertEquals(ImmutableSet.of(new SimpleCoding(SYSTEM_1, "code1"),
        new SimpleCoding(SYSTEM_2, "code2", "v1")), actualCodingSet);

  }
}
