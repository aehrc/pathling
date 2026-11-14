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

package au.csiro.pathling.sql.udf;

import static au.csiro.pathling.test.helpers.FhirMatchers.deepEq;
import static au.csiro.pathling.test.helpers.TerminologyServiceHelpers.setupLookup;
import static org.hl7.fhir.r4.model.codesystems.ConceptMapEquivalence.EQUIVALENT;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.fhirpath.encoding.CodingSchema;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.terminology.TerminologyService.Property;
import au.csiro.pathling.terminology.TerminologyService.Translation;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.test.AbstractTerminologyTestBase;
import au.csiro.pathling.test.helpers.TerminologyServiceHelpers;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.stream.Stream;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.StringType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.collection.mutable.ArraySeq;

/**
 * Tests each terminology operation against a Coding column narrower than the canonical structure
 * (T085, FR-032).
 *
 * <p>A pruned new-layout schema carries only the elements the data populates, so a Coding that has
 * a system and a code and nothing else is stored as a structure of those two fields. The fields are
 * given here in the reverse of their canonical order, as a row carrying its schema, which is what
 * Spark hands a user-defined function. Every operation must read the fields by name and treat the
 * absent ones as null, so the terminology service sees the same Coding it would see for the
 * canonical structure.
 *
 * @author Piotr Szul
 */
class NarrowCodingTest extends AbstractTerminologyTestBase {

  private static final StructType NARROW_SCHEMA =
      new StructType()
          .add(CodingSchema.CODE_FIELD, DataTypes.StringType)
          .add(CodingSchema.SYSTEM_FIELD, DataTypes.StringType);

  /** The Codings as the narrow structure can hold them: a system and a code only. */
  private static final Coding NARROW_A = new Coding(SYSTEM_A, CODE_A, null);

  private static final Coding NARROW_B = new Coding(SYSTEM_A, CODE_B, null);

  private static final Coding NARROW_C = new Coding(SYSTEM_C, CODE_C, null);

  private static final String VALUE_SET_URL = "uuid:vs";

  private static final String CONCEPT_MAP_URL = "uuid:cm";

  private TerminologyService terminologyService;

  private TerminologyServiceFactory terminologyServiceFactory;

  @BeforeEach
  void setUp() {
    terminologyService = mock(TerminologyService.class);
    terminologyServiceFactory = mock(TerminologyServiceFactory.class);
    when(terminologyServiceFactory.build()).thenReturn(terminologyService);
  }

  @Test
  void subsumesReadsNarrowCodings() {
    TerminologyServiceHelpers.setupSubsumes(terminologyService).withSubsumes(NARROW_A, NARROW_B);
    final SubsumesUdf udf = new SubsumesUdf(terminologyServiceFactory);

    assertTrue(udf.call(narrow(NARROW_A), narrow(NARROW_B), false));
    assertFalse(udf.call(narrow(NARROW_B), narrow(NARROW_A), false));
    assertTrue(udf.call(narrowMany(NARROW_C, NARROW_A), narrow(NARROW_B), false));
    assertFalse(udf.call(narrow(NARROW_A), narrowMany(NARROW_C, NARROW_B), true));
  }

  @Test
  void memberOfReadsNarrowCodings() {
    TerminologyServiceHelpers.setupValidate(terminologyService)
        .withValueSet(VALUE_SET_URL, NARROW_A);
    final MemberOfUdf udf = new MemberOfUdf(terminologyServiceFactory);

    assertTrue(udf.call(narrow(NARROW_A), VALUE_SET_URL));
    assertFalse(udf.call(narrow(NARROW_B), VALUE_SET_URL));
    assertTrue(udf.call(narrowMany(NARROW_B, NARROW_A), VALUE_SET_URL));
  }

  @Test
  void translateReadsNarrowCodings() {
    TerminologyServiceHelpers.setupTranslate(terminologyService)
        .withTranslations(NARROW_A, CONCEPT_MAP_URL, Translation.of(EQUIVALENT, CODING_BB));
    final TranslateUdf udf = new TranslateUdf(terminologyServiceFactory);

    assertTranslatesTo(CODING_BB, udf.call(narrow(NARROW_A), CONCEPT_MAP_URL, false, null, null));
    assertTranslatesTo(
        CODING_BB, udf.call(narrowMany(NARROW_C, NARROW_A), CONCEPT_MAP_URL, false, null, null));
  }

  @Test
  void displayReadsANarrowCoding() {
    setupLookup(terminologyService).withDisplay(NARROW_A, "Display A");
    final DisplayUdf udf = new DisplayUdf(terminologyServiceFactory);

    assertEquals("Display A", udf.call(narrow(NARROW_A), null));
  }

  @Test
  void propertyReadsANarrowCoding() {
    when(terminologyService.lookup(deepEq(NARROW_A), eq("prop"), eq(null)))
        .thenReturn(List.of(Property.of("prop", new StringType("value A"))));
    final PropertyUdf udf = PropertyUdf.forType(terminologyServiceFactory, FHIRDefinedType.STRING);

    assertArrayEquals(new String[] {"value A"}, udf.call(narrow(NARROW_A), "prop", null));
  }

  @Test
  void designationReadsNarrowCodings() {
    setupLookup(terminologyService)
        .withDesignation(NARROW_A, NARROW_C, "en", "A in C")
        .withDesignation(NARROW_A, NARROW_B, "en", "A in B")
        .done();
    final DesignationUdf udf = new DesignationUdf(terminologyServiceFactory);

    assertArrayEquals(new String[] {"A in C"}, udf.call(narrow(NARROW_A), narrow(NARROW_C), "en"));
    assertArrayEquals(new String[] {"A in C", "A in B"}, udf.call(narrow(NARROW_A), null, "en"));
  }

  /** Stores a Coding in the narrow structure, as a row carrying that structure's schema. */
  @Nonnull
  private static Row narrow(@Nonnull final Coding coding) {
    return new GenericRowWithSchema(
        new Object[] {coding.getCode(), coding.getSystem()}, NARROW_SCHEMA);
  }

  @Nonnull
  private static ArraySeq<Object> narrowMany(@Nonnull final Coding... codings) {
    return ArraySeq.make(Stream.of(codings).map(NarrowCodingTest::narrow).toArray(Row[]::new));
  }

  private static void assertTranslatesTo(
      @Nonnull final Coding expected, @Nonnull final Row[] rows) {
    final List<Coding> translated = Stream.of(rows).map(CodingSchema::decode).toList();
    assertEquals(1, translated.size());
    assertTrue(expected.equalsDeep(translated.get(0)));
  }
}
