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

package au.csiro.pathling.fhirpath.ansi;

import static au.csiro.pathling.views.FhirView.columns;
import static org.junit.jupiter.api.Assertions.assertEquals;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.datasource.ObjectDataSource;
import au.csiro.pathling.views.Column;
import au.csiro.pathling.views.ColumnTag;
import au.csiro.pathling.views.FhirView;
import au.csiro.pathling.views.FhirViewExecutor;
import jakarta.annotation.Nonnull;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.IntegerType;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Observation.ObservationComponentComponent;
import org.hl7.fhir.r4.model.Quantity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Pins FHIRPath and ViewDefinition evaluation to produce <em>identical</em> results under {@code
 * spark.sql.ansi.enabled} set to either {@code true} or {@code false} (FR-001, FR-010). Each
 * scenario is evaluated twice - once with ANSI on, once with ANSI off - by rebuilding the entire
 * query (source encoding and view) under each setting, then asserting the result sets are equal.
 *
 * <p>The decimal-overflow and out-of-range scenarios fail today under ANSI-on (the divergence this
 * feature removes); after the fix they yield empty/NULL identically under both settings.
 *
 * @author John Grimes
 */
@SpringBootUnitTest
class AnsiModeIndependenceTest {

  @Autowired FhirEncoders fhirEncoders;

  @Autowired SparkSession spark;

  private String originalAnsiSetting;

  @AfterEach
  void restoreAnsiSetting() {
    if (originalAnsiSetting != null) {
      spark.conf().set("spark.sql.ansi.enabled", originalAnsiSetting);
    }
  }

  /**
   * Builds and runs the view over the given resources under the requested ANSI setting. The source
   * encoding and the view query are both constructed after the setting is applied, so the result
   * reflects that setting end to end.
   */
  @Nonnull
  private List<String> evalUnderAnsi(
      final boolean ansiEnabled,
      @Nonnull final List<IBaseResource> resources,
      @Nonnull final FhirView view) {
    if (originalAnsiSetting == null) {
      originalAnsiSetting = spark.conf().get("spark.sql.ansi.enabled");
    }
    spark.conf().set("spark.sql.ansi.enabled", ansiEnabled);
    final ObjectDataSource dataSource = new ObjectDataSource(spark, fhirEncoders, resources);
    final FhirViewExecutor executor = new FhirViewExecutor(fhirEncoders.getContext(), dataSource);
    return executor.buildQuery(view).collectAsList().stream()
        .map(Row::toString)
        .collect(Collectors.toList());
  }

  /** Asserts the result set is identical whether ANSI mode is enabled or disabled. */
  private void assertParity(
      @Nonnull final List<IBaseResource> resources,
      @Nonnull final FhirView view,
      @Nonnull final String message) {
    final List<String> ansiOn = evalUnderAnsi(true, resources, view);
    final List<String> ansiOff = evalUnderAnsi(false, resources, view);
    assertEquals(ansiOn, ansiOff, message);
  }

  @Nonnull
  private static FhirView singleColumn(@Nonnull final String expression) {
    return FhirView.ofResource("Observation")
        .select(columns(Column.builder().name("value").path(expression).collection(false).build()))
        .build();
  }

  @Nonnull
  private static Observation observationWithQuantity(@Nonnull final BigDecimal value) {
    return (Observation)
        new Observation().setValue(new Quantity().setValue(value)).setId("Observation/1");
  }

  // ---------------------------------------------------------------------------
  // User Story 1: identical results regardless of the ANSI setting.
  // ---------------------------------------------------------------------------

  @Test
  void decimalMultiplicationOverflowIsIdenticalUnderBothSettings() {
    // 1e14 * 1e14 = 1e28, which exceeds the 26 integer digits of DECIMAL(32,6); the result must be
    // empty under both settings rather than aborting under ANSI-on.
    final List<IBaseResource> resources = List.of(observationWithQuantity(new BigDecimal("1E14")));
    assertParity(
        resources,
        singleColumn("value.ofType(Quantity).value * value.ofType(Quantity).value"),
        "decimal multiplication overflow");
  }

  @Test
  void decimalAdditionIsIdenticalUnderBothSettings() {
    // A valid decimal addition must yield the same (non-empty) value under both settings.
    final List<IBaseResource> resources = List.of(observationWithQuantity(new BigDecimal("23.40")));
    assertParity(
        resources, singleColumn("value.ofType(Quantity).value + 1.5"), "valid decimal addition");
  }

  @Test
  void decimalCombineNormalisationIsIdenticalUnderBothSettings() {
    // combine() routes decimals through normalizeDecimalType (a DECIMAL(32,6) cast).
    final List<IBaseResource> resources = List.of(observationWithQuantity(new BigDecimal("23.40")));
    assertParity(
        resources,
        FhirView.ofResource("Observation")
            .select(
                columns(
                    Column.builder()
                        .name("value")
                        .path("value.ofType(Quantity).value.combine(1.5)")
                        .collection(true)
                        .build()))
            .build(),
        "decimal combine normalisation");
  }

  @Test
  void mixedNumericCoercionIsIdenticalUnderBothSettings() {
    // Integer + Decimal coerces the integer to decimal through castAs; results must match.
    final Observation observation = observationWithQuantity(new BigDecimal("23.40"));
    observation.addComponent(
        new ObservationComponentComponent(new CodeableConcept().setText("int"))
            .setValue(new IntegerType(7)));
    assertParity(
        List.of(observation),
        singleColumn(
            "component.where(code.text='int').value.ofType(integer) +"
                + " value.ofType(Quantity).value"),
        "mixed integer/decimal coercion");
  }

  @Test
  void outOfRangeQuantityValueIsIdenticalUnderBothSettings() {
    // A Quantity value outside the representable DECIMAL(32,6) range must be empty under both
    // settings rather than aborting under ANSI-on.
    final List<IBaseResource> resources = List.of(observationWithQuantity(new BigDecimal("1E30")));
    assertParity(
        resources, singleColumn("value.ofType(Quantity).value"), "out-of-range Quantity value");
  }

  @Test
  void nonConformingViewColumnsAreIdenticalUnderBothSettings() {
    // A view whose declared column types do not match the input values: the non-conforming cells
    // must be empty under both settings, with the query completing.
    final Observation observation =
        (Observation)
            new Observation()
                .setCode(new CodeableConcept().setText("not-a-number"))
                .setId("Observation/1");
    final FhirView view =
        FhirView.ofResource("Observation")
            .select(
                columns(
                    typedColumn("asInt", "code.text", "INT"),
                    typedColumn("asDecimal", "code.text", "DECIMAL(10,2)"),
                    typedColumn("asDate", "code.text", "DATE")))
            .build();
    assertParity(List.of(observation), view, "non-conforming view columns");
  }

  @Nonnull
  private static Column typedColumn(
      @Nonnull final String name, @Nonnull final String path, @Nonnull final String ansiType) {
    return Column.builder()
        .name(name)
        .path(path)
        .collection(false)
        .tag(List.of(ColumnTag.of(ColumnTag.ANSI_TYPE_TAG, ansiType)))
        .build();
  }

  // ---------------------------------------------------------------------------
  // User Story 2: correct values on a stock Spark 4 session (ANSI on, the default).
  //
  // These assert the specific expected output under ANSI-on (not merely parity): a non-conforming
  // or out-of-range value yields an empty cell while the query completes for every other row.
  // ---------------------------------------------------------------------------

  @Test
  void decimalOverflowProducesEmptyUnderAnsiOn() {
    final List<String> result =
        evalUnderAnsi(
            true,
            List.of(observationWithQuantity(new BigDecimal("1E14"))),
            singleColumn("value.ofType(Quantity).value * value.ofType(Quantity).value"));
    assertEquals(List.of("[null]"), result, "decimal overflow yields an empty cell under ANSI-on");
  }

  @Test
  void nonConformingViewColumnYieldsEmptyCellAndQueryCompletesUnderAnsiOn() {
    // One row has a value that conforms to the declared INT column, the other does not. Under
    // ANSI-on the non-conforming cell must be empty while the conforming row survives.
    final Observation conforming =
        (Observation)
            new Observation().setCode(new CodeableConcept().setText("123")).setId("Observation/1");
    final Observation nonConforming =
        (Observation)
            new Observation().setCode(new CodeableConcept().setText("abc")).setId("Observation/2");
    final FhirView view =
        FhirView.ofResource("Observation")
            .select(columns(typedColumn("v", "code.text", "INT")))
            .build();

    final List<String> result =
        new ArrayList<>(evalUnderAnsi(true, List.of(conforming, nonConforming), view));
    Collections.sort(result);
    assertEquals(
        List.of("[123]", "[null]"),
        result,
        "non-conforming cell is empty and the conforming row survives under ANSI-on");
  }

  @Test
  void conversionFunctionOnInvalidInputProducesEmptyUnderAnsiOn() {
    final List<String> result =
        evalUnderAnsi(
            true,
            List.of(observationWithQuantity(new BigDecimal("1"))),
            singleColumn("'abc'.toInteger()"));
    assertEquals(
        List.of("[null]"), result, "conversion of invalid input yields empty under ANSI-on");
  }
}
