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

import static au.csiro.pathling.test.helpers.TerminologyServiceHelpers.setupLookup;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.test.AbstractTerminologyTestBase;
import au.csiro.pathling.test.helpers.TerminologyServiceHelpers;
import jakarta.annotation.Nonnull;
import java.util.stream.Stream;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.r4.model.Coding;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.collection.mutable.ArraySeq;

/**
 * Tests the terminology operations against a Coding held in an unnamed structure, which is read by
 * the positions the released versions read every Coding by.
 *
 * <p>The R API builds a Coding with an unnamed Spark {@code struct}, so its fields are named {@code
 * col1} to {@code col6}, and the id, display and userSelected it leaves out are untyped nulls. None
 * of those names is the name of a Coding field, so each operation must fall back to reading the
 * system, version and code by position.
 *
 * @author Piotr Szul
 */
class PositionalCodingTest extends AbstractTerminologyTestBase {

  private static final StructType POSITIONAL_SCHEMA =
      new StructType()
          .add("col1", DataTypes.NullType)
          .add("col2", DataTypes.StringType)
          .add("col3", DataTypes.StringType)
          .add("col4", DataTypes.StringType)
          .add("col5", DataTypes.NullType)
          .add("col6", DataTypes.NullType);

  /** The Codings as the positional structure holds them: a system and a code only. */
  private static final Coding POSITIONAL_A = new Coding(SYSTEM_A, CODE_A, null);

  private static final Coding POSITIONAL_B = new Coding(SYSTEM_A, CODE_B, null);

  private static final String VALUE_SET_URL = "uuid:vs";

  private TerminologyService terminologyService;

  private TerminologyServiceFactory terminologyServiceFactory;

  @BeforeEach
  void setUp() {
    terminologyService = mock(TerminologyService.class);
    terminologyServiceFactory = mock(TerminologyServiceFactory.class);
    when(terminologyServiceFactory.build()).thenReturn(terminologyService);
  }

  @Test
  void memberOfReadsAPositionalCoding() {
    TerminologyServiceHelpers.setupValidate(terminologyService)
        .withValueSet(VALUE_SET_URL, POSITIONAL_A);
    final MemberOfUdf udf = new MemberOfUdf(terminologyServiceFactory);

    assertTrue(udf.call(positional(POSITIONAL_A), VALUE_SET_URL));
    assertFalse(udf.call(positional(POSITIONAL_B), VALUE_SET_URL));
    assertTrue(udf.call(positionalMany(POSITIONAL_B, POSITIONAL_A), VALUE_SET_URL));
  }

  @Test
  void displayReadsAPositionalCoding() {
    setupLookup(terminologyService).withDisplay(POSITIONAL_A, "Display A");
    final DisplayUdf udf = new DisplayUdf(terminologyServiceFactory);

    assertEquals("Display A", udf.call(positional(POSITIONAL_A), null));
  }

  @Test
  void subsumesReadsPositionalCodings() {
    TerminologyServiceHelpers.setupSubsumes(terminologyService)
        .withSubsumes(POSITIONAL_A, POSITIONAL_B);
    final SubsumesUdf udf = new SubsumesUdf(terminologyServiceFactory);

    assertTrue(udf.call(positional(POSITIONAL_A), positional(POSITIONAL_B), false));
    assertFalse(udf.call(positional(POSITIONAL_B), positional(POSITIONAL_A), false));
  }

  @Test
  void memberOfRejectsAStructureThatIsNeitherNamedNorPositional() {
    // Two unnamed fields are too few to be read by the released positions.
    final StructType schema =
        new StructType().add("col1", DataTypes.StringType).add("col2", DataTypes.StringType);
    final Row row = new GenericRowWithSchema(new Object[] {SYSTEM_A, CODE_A}, schema);
    final MemberOfUdf udf = new MemberOfUdf(terminologyServiceFactory);

    final IllegalArgumentException error =
        assertThrows(IllegalArgumentException.class, () -> udf.call(row, VALUE_SET_URL));
    assertTrue(error.getMessage().contains("col2"), error.getMessage());
  }

  /** Stores a Coding in the positional structure, as a row carrying that structure's schema. */
  @Nonnull
  private static Row positional(@Nonnull final Coding coding) {
    return new GenericRowWithSchema(
        new Object[] {null, coding.getSystem(), coding.getVersion(), coding.getCode(), null, null},
        POSITIONAL_SCHEMA);
  }

  @Nonnull
  private static ArraySeq<Object> positionalMany(@Nonnull final Coding... codings) {
    return ArraySeq.make(
        Stream.of(codings).map(PositionalCodingTest::positional).toArray(Row[]::new));
  }
}
