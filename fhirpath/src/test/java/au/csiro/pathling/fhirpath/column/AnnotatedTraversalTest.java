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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import au.csiro.pathling.schema.LayoutFields;
import au.csiro.pathling.schema.annotation.AnnotationKind;
import au.csiro.pathling.schema.annotation.AnnotationSwitch;
import au.csiro.pathling.test.SpringBootUnitTest;
import jakarta.annotation.Nonnull;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Tests that the engine pairs a primitive's values with their annotations by position, reading a
 * stored annotation where the schema carries it and computing it otherwise, through the Catalyst
 * resolver (decision 135).
 */
@SpringBootUnitTest
class AnnotatedTraversalTest {

  private static final String START = LayoutFields.START_ANNOTATION;

  /**
   * A stub kind, whose computed annotation is the value marked, so it is told from a stored one.
   */
  private static final AnnotationKind STUB =
      new AnnotationKind(AnnotationSwitch.RANGE, List.of(START)) {
        @Override
        @Nonnull
        public Column compute(@Nonnull final Column value) {
          return functions.struct(functions.concat(value, functions.lit("~")).alias(START));
        }
      };

  @Autowired SparkSession spark;

  @Nonnull
  private List<String> annotated(
      @Nonnull final String parent, @Nonnull final String field, final boolean repeating) {
    return annotated(parent, field, repeating, Optional.empty());
  }

  @Nonnull
  private List<String> annotated(
      @Nonnull final String parent,
      @Nonnull final String field,
      final boolean repeating,
      @Nonnull final Optional<FHIRDefinedType> fhirType) {
    final ColumnRepresentation pairs =
        new DefaultRepresentation(functions.expr(parent))
            .traverseAnnotated(field, fhirType, DataTypes.StringType, repeating, STUB, START);
    final Row result = spark.range(1).select(pairs.getValue()).first();
    // A singular field yields one item rather than an array of them.
    final List<Row> items =
        result.get(0) instanceof final Row item ? List.of(item) : result.<Row>getList(0);
    return items.stream().map(pair -> pair.get(0) + "=" + pair.getString(1)).toList();
  }

  @Test
  void pairsEachValueOfARepeatingPrimitiveWithItsOwnStoredAnnotation() {
    final String timing =
        "named_struct('event', array('a', 'b', 'c'), '_event', array(named_struct('__start',"
            + " 'A'), null, named_struct('__start', 'C')))";
    assertThat(annotated(timing, "event", true)).containsExactly("a=A", "b=null", "c=C");
    // Read through the traversal, the annotations lose their positions, because the traversal
    // removes the null that stands for the middle value's absent group.
    final Column traversed =
        new DefaultRepresentation(functions.expr(timing))
            .traverse("_event", Optional.empty(), DataTypes.NullType)
            .traverse(START, Optional.empty(), DataTypes.StringType)
            .getValue();
    assertThat(spark.range(1).select(traversed).first().<String>getList(0))
        .containsExactly("A", "C");
  }

  @Test
  void computesTheAnnotationsOfARepeatingPrimitiveWhereTheSchemaLacksThem() {
    assertThat(annotated("named_struct('event', array('a', null, 'c'))", "event", true))
        .containsExactly("a=a~", "c=c~");
    assertThat(
            annotated(
                "named_struct('event', array('a', 'b'), '_event', array(named_struct('id', 'x'),"
                    + " null))",
                "event",
                true))
        .containsExactly("a=a~", "b=b~");
  }

  @Test
  void readsAPresentAnnotationOfASingularPrimitiveAndComputesAnAbsentOne() {
    assertThat(
            annotated(
                "named_struct('start', 'a', '_start', named_struct('__start', 'A'))",
                "start",
                false))
        .containsExactly("a=A");
    assertThat(annotated("named_struct('start', 'a')", "start", false)).containsExactly("a=a~");
  }

  @Test
  void pairsTheValuesBeneathARepeatingParent() {
    final String periods =
        "array(named_struct('start', 'a', '_start', named_struct('__start', 'A')),"
            + " named_struct('start', cast(null as string), '_start', named_struct('__start',"
            + " 'X')), named_struct('start', 'c', '_start', cast(null as struct<__start:"
            + " string>)))";
    assertThat(annotated(periods, "start", false)).containsExactly("a=A", "c=null");
  }

  @Test
  void decodesEachValueByItsTypeAndComputesItsAnnotationFromTheStoredValue() {
    // A base64Binary is stored as binary, and decoded to its base64 text. The bytes of "ABC" have
    // a base64 form without padding.
    final Row pair =
        spark
            .range(1)
            .select(
                new DefaultRepresentation(functions.expr("named_struct('value', X'414243')"))
                    .traverseAnnotated(
                        "value",
                        Optional.of(FHIRDefinedType.BASE64BINARY),
                        DataTypes.BinaryType,
                        false,
                        STUB,
                        START)
                    .getValue())
            .first()
            .getStruct(0);
    assertThat(pair.getString(0)).isEqualTo("QUJD");
    assertThat(pair.getString(1)).isEqualTo("ABC~");
  }

  @Test
  void refusesToPairADecimal() {
    final DefaultRepresentation parent =
        new DefaultRepresentation(functions.expr("named_struct('value', '1.5')"));
    assertThatThrownBy(
            () ->
                parent.traverseAnnotated(
                    "value",
                    Optional.of(FHIRDefinedType.DECIMAL),
                    DataTypes.StringType,
                    false,
                    STUB,
                    START))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("its value is its numeric annotation, which traverse reads");
  }

  @Test
  void readsOnlyTheAnnotationFieldOfTheGroupsOfARepeatingPrimitive(@TempDir final Path dir) {
    final String path = dir.resolve("timing").toString();
    spark
        .range(1)
        .selectExpr(
            "named_struct('event', array('a'), '_event', array(named_struct('id', 'i',"
                + " 'extension', array(named_struct('url', 'u')), '__start', 'A'))) as timing")
        .write()
        .parquet(path);
    final Column pairs =
        new DefaultRepresentation(functions.col("timing"))
            .traverseAnnotated("event", Optional.empty(), DataTypes.StringType, true, STUB, START)
            .getValue();

    assertThat(spark.read().parquet(path).select(pairs).queryExecution().executedPlan().toString())
        .contains(
            "ReadSchema: struct<timing:struct<event:array<string>,_event:array<struct<"
                + START
                + ":string>>>>");
  }
}
