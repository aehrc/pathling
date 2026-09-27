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

package au.csiro.pathling.fhirpath.column;

import static org.apache.spark.sql.functions.col;
import static org.assertj.core.api.Assertions.assertThat;

import au.csiro.pathling.test.SpringBootUnitTest;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Tests that traversal to a primitive retains the parent and the element's name, so that a named
 * sibling of the element within its parent can be resolved (T094, R-014).
 *
 * <p>The sibling here is a metadata group named after the element with a leading underscore, as the
 * new layout stores a primitive's id and extensions. Nothing populates the group until M5, so the
 * data is built by hand.
 *
 * @author Piotr Szul
 */
@SpringBootUnitTest
class ElementRepresentationTest {

  @Autowired SparkSession spark;

  private Dataset<Row> dataset;

  @BeforeEach
  void setUp() {
    dataset =
        spark.sql(
            "select 'r1' as id, '2000-01-01' as birthDate,"
                + " named_struct('id', 'b1') as _birthDate,"
                + " named_struct('family', 'F1', '_family', named_struct('id', 'f1')) as single,"
                + " array(named_struct('family', 'F2', '_family', named_struct('id', 'f2')),"
                + "   named_struct('family', 'F3', '_family', named_struct('id', 'f3'))) as many,"
                + " named_struct('value', '1.5', '_value', named_struct('id', 'v1')) as quantity");
  }

  @Test
  void siblingUnderSingularParent() {
    final ColumnRepresentation family =
        new DefaultRepresentation(col("single"))
            .traverse("family", Optional.of(FHIRDefinedType.STRING));
    assertThat(family).isInstanceOf(ElementRepresentation.class);
    assertThat(((ElementRepresentation) family).getElementName()).isEqualTo("family");
    assertThat(values(sibling(family, "_family").traverse("id"))).containsExactly("f1");
  }

  @Test
  void siblingUnderRepeatingParent() {
    final ColumnRepresentation family =
        new DefaultRepresentation(col("many"))
            .traverse("family", Optional.of(FHIRDefinedType.STRING));
    assertThat(values(sibling(family, "_family").traverse("id")))
        .containsExactly("ArraySeq(f2, f3)");
  }

  @Test
  void siblingAtTheResourceRoot() {
    final ColumnRepresentation birthDate =
        ResourceRepresentation.withIdColumn()
            .traverse("birthDate", Optional.of(FHIRDefinedType.DATE));
    assertThat(values(sibling(birthDate, "_birthDate").traverse("id"))).containsExactly("b1");
  }

  @Test
  void siblingOfDecodedDecimal() {
    // A decimal is decoded at traversal, and still retains its parent.
    final ColumnRepresentation value =
        new DefaultRepresentation(col("quantity"))
            .traverse("value", Optional.of(FHIRDefinedType.DECIMAL));
    assertThat(values(value)).containsExactly("1.500000");
    assertThat(values(sibling(value, "_value").traverse("id"))).containsExactly("v1");
  }

  @Test
  void absentSiblingIsNull() {
    final ColumnRepresentation family =
        new DefaultRepresentation(col("single"))
            .traverse("family", Optional.of(FHIRDefinedType.STRING));
    assertThat(values(sibling(family, "_missing"))).containsExactly("null");
  }

  @Test
  void complexElementDoesNotRetainItsParent() {
    assertThat(
            new DefaultRepresentation(col("many"))
                .traverse("family", Optional.of(FHIRDefinedType.HUMANNAME)))
        .isNotInstanceOf(ElementRepresentation.class);
  }

  @Test
  void operationOnTheElementYieldsAnOrdinaryRepresentation() {
    final ColumnRepresentation family =
        new DefaultRepresentation(col("many"))
            .traverse("family", Optional.of(FHIRDefinedType.STRING));
    assertThat(family.first()).isNotInstanceOf(ElementRepresentation.class);
  }

  @Nonnull
  private static ColumnRepresentation sibling(
      @Nonnull final ColumnRepresentation element, @Nonnull final String name) {
    assertThat(element).isInstanceOf(ElementRepresentation.class);
    return ((ElementRepresentation) element).traverseSibling(name);
  }

  @Nonnull
  private List<String> values(@Nonnull final ColumnRepresentation representation) {
    return dataset.select(representation.getValue()).collectAsList().stream()
        .map(row -> Objects.toString(row.get(0)))
        .toList();
  }
}
