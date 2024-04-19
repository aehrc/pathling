/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.fhirpath.operator;

import static au.csiro.pathling.test.assertions.Assertions.assertThat;
import static au.csiro.pathling.test.helpers.SparkHelpers.quantityStructType;
import static au.csiro.pathling.test.helpers.SparkHelpers.rowForUcumQuantity;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.builders.ElementPathBuilder;
import au.csiro.pathling.test.builders.ParserContextBuilder;
import ca.uhn.fhir.context.FhirContext;
import com.google.common.collect.ImmutableSet;
import jakarta.annotation.Nonnull;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.fhir.ucum.Prefix;
import org.fhir.ucum.UcumService;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

@SpringBootUnitTest
public class QuantityOperatorsPrecisionTest {

  @Autowired
  SparkSession spark;

  @Autowired
  FhirContext fhirContext;

  @Autowired
  UcumService ucumService;

  static final String ID_ALIAS = "_abc123";

  // Reasonable decimal with units assume above the value of 9999 we would use the next prefix up
  // (if available)

  static final String REASONABLE_DECIMAL_01 = createSpanningDecimal(9, 3, 1,
      6).toString(); // 9000.000001
  static final String REASONABLE_DECIMAL_02 = createSpanningDecimal(9, 3, 2,
      6).toString(); // 9000.000002

  // for Decimal(32,6)
  static final String FULL_DECIMAL_01 = createSpanningDecimal(9, 26, 1,
      6).toString(); // 9e26 + 0.00001 
  static final String FULL_DECIMAL_02 = createSpanningDecimal(9, 26, 2,
      6).toString(); // 9e26 + 0.00002

  // These units (prefixes) results in overflow for largest supported decimals (e.g.  9e26 'Tm') 
  static final Set<String> UNSUPPORTED_FULL_DECIMAL_UNITS = ImmutableSet.of("Ym", "Zm", "Em", "Pm",
      "Tm");

  // These mol prefixes results in overflow for reasonable decimals (e.g.  9e3 'Tmol')
  static final Set<String> UNSUPPORTED_REASONABLE_DECIMAL_MOL_UNITS = ImmutableSet.of("Ymol",
      "Zmol",
      "Emol", "Pmol", "Tmol");


  @Nonnull
  private static String unitToRowId(@Nonnull final String unit) {
    return "unit-" + unit;
  }

  @Nonnull
  private ElementPath buildQuantityPathForUnits(@Nonnull final String value,
      final List<String> units) {
    DatasetBuilder datasetBuilder = new DatasetBuilder(spark)
        .withIdColumn(ID_ALIAS)
        .withStructTypeColumns(quantityStructType());
    for (final String unit : units) {
      datasetBuilder = datasetBuilder.withRow(unitToRowId(unit), rowForUcumQuantity(value, unit));
    }
    final Dataset<Row> dataset = datasetBuilder.buildWithStructValue();
    return new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.QUANTITY)
        .singular(true)
        .dataset(dataset)
        .idAndValueColumns()
        .build();
  }

  @Nonnull
  private List<String> getAllPrefixedUnits(@Nonnull final String baseUnit) {
    return ucumService.getModel().getPrefixes().stream()
        .map(Prefix::getCode)
        .filter(p -> p.length() == 1) // filter out Ki, Gi etc
        .map(p -> p + baseUnit)
        .collect(Collectors.toUnmodifiableList());
  }

  @SuppressWarnings("SameParameterValue")
  @Nonnull
  private static BigDecimal createSpanningDecimal(final int leftValue, final int leftScale,
      final int rightValue, final int rightScale) {
    return new BigDecimal(leftValue).movePointRight(leftScale)
        .add(new BigDecimal(rightValue).movePointLeft(rightScale));
  }

  @SuppressWarnings("SameParameterValue")
  @Nonnull
  private static List<Row> createResult(@Nonnull final List<String> unitRange,
      final boolean result) {
    return createResult(unitRange, result, Collections.emptySet());
  }

  @Nonnull
  private static List<Row> createResult(@Nonnull final List<String> unitRange, final boolean result,
      @Nonnull final Set<String> outOfRangeUnits) {
    return unitRange.stream().map(
        unit ->
            RowFactory.create(unitToRowId(unit), outOfRangeUnits.contains(unit)
                                                 ? null
                                                 : result)).collect(Collectors.toList());
  }


  @Nonnull
  private FhirPath callOperator(@Nonnull final ElementPath left, @Nonnull final String operator,
      @Nonnull final ElementPath right) {
    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext)
        .groupingColumns(Collections.singletonList(left.getIdColumn()))
        .build();

    final OperatorInput input = new OperatorInput(parserContext, left, right);
    final Operator equalityOperator = Operator.getInstance(operator);
    return equalityOperator.invoke(input);
  }

  @Test
  void equalityPrecisionForReasonableDecimals() {
    final List<String> unitRange = getAllPrefixedUnits("m");
    final ElementPath left = buildQuantityPathForUnits(REASONABLE_DECIMAL_01, unitRange);
    final ElementPath right = buildQuantityPathForUnits(REASONABLE_DECIMAL_01, unitRange);
    final FhirPath result = callOperator(left, "=", right);
    assertThat(result).selectResult().hasRows(createResult(unitRange, true));
  }


  @Test
  void nonEqualityPrecisionForReasonableDecimals() {
    final List<String> unitRange = getAllPrefixedUnits("m");
    final ElementPath left = buildQuantityPathForUnits(REASONABLE_DECIMAL_01, unitRange);
    final ElementPath right = buildQuantityPathForUnits(REASONABLE_DECIMAL_02, unitRange);
    final FhirPath result = callOperator(left, "!=", right);
    assertThat(result).selectResult().hasRows(createResult(unitRange, true));
  }


  @Test
  void comparisonPrecisionForReasonableDecimals() {
    final List<String> unitRange = getAllPrefixedUnits("m");
    final ElementPath left = buildQuantityPathForUnits(REASONABLE_DECIMAL_01, unitRange);
    final ElementPath right = buildQuantityPathForUnits(REASONABLE_DECIMAL_02, unitRange);
    final FhirPath result = callOperator(left, "<", right);
    assertThat(result).selectResult().hasRows(createResult(unitRange, true));
  }


  @Test
  void equalityPrecisionForFullDecimals() {
    final List<String> unitRange = getAllPrefixedUnits("m");
    final ElementPath left = buildQuantityPathForUnits(FULL_DECIMAL_01, unitRange);
    final ElementPath right = buildQuantityPathForUnits(FULL_DECIMAL_01, unitRange);
    final FhirPath result = callOperator(left, "=", right);
    assertThat(result).selectResult()
        .hasRows(createResult(unitRange, true, UNSUPPORTED_FULL_DECIMAL_UNITS));
  }

  @Test
  void nonEqualityPrecisionForFullDecimals() {
    final List<String> unitRange = getAllPrefixedUnits("m");
    final ElementPath left = buildQuantityPathForUnits(FULL_DECIMAL_01, unitRange);
    final ElementPath right = buildQuantityPathForUnits(FULL_DECIMAL_02, unitRange);
    final FhirPath result = callOperator(left, "!=", right);
    assertThat(result).selectResult()
        .hasRows(createResult(unitRange, true, UNSUPPORTED_FULL_DECIMAL_UNITS));
  }

  @Test
  void comparisonPrecisionForFullDecimals() {
    final List<String> unitRange = getAllPrefixedUnits("m");
    final ElementPath left = buildQuantityPathForUnits(FULL_DECIMAL_01, unitRange);
    final ElementPath right = buildQuantityPathForUnits(FULL_DECIMAL_02, unitRange);
    final FhirPath result = callOperator(left, "<", right);
    assertThat(result).selectResult()
        .hasRows(createResult(unitRange, true, UNSUPPORTED_FULL_DECIMAL_UNITS));
  }

  @Test
  void equalityPrecisionForReasonableDecimalsWithMoles() {
    final List<String> unitRange = getAllPrefixedUnits("mol");
    final ElementPath left = buildQuantityPathForUnits(REASONABLE_DECIMAL_01, unitRange);
    final ElementPath right = buildQuantityPathForUnits(REASONABLE_DECIMAL_01, unitRange);
    final FhirPath result = callOperator(left, "=", right);
    assertThat(result).selectResult()
        .hasRows(createResult(unitRange, true, UNSUPPORTED_REASONABLE_DECIMAL_MOL_UNITS));
  }

}
