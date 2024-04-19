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

package au.csiro.pathling.fhirpath.parser;

import static au.csiro.pathling.test.fixtures.PatientListBuilder.PATIENT_ID_121503c8;
import static au.csiro.pathling.test.fixtures.PatientListBuilder.PATIENT_ID_2b36c1e2;
import static au.csiro.pathling.test.fixtures.PatientListBuilder.PATIENT_ID_7001ad9c;
import static au.csiro.pathling.test.fixtures.PatientListBuilder.PATIENT_ID_8ee183e2;
import static au.csiro.pathling.test.fixtures.PatientListBuilder.PATIENT_ID_9360820c;
import static au.csiro.pathling.test.fixtures.PatientListBuilder.PATIENT_ID_bbd33563;
import static au.csiro.pathling.test.fixtures.PatientListBuilder.PATIENT_ID_beff242e;
import static au.csiro.pathling.test.fixtures.PatientListBuilder.allPatientsWithValue;
import static au.csiro.pathling.test.helpers.TerminologyHelpers.CD_SNOMED_195662009;
import static au.csiro.pathling.test.helpers.TerminologyHelpers.CD_SNOMED_284551006;
import static au.csiro.pathling.test.helpers.TerminologyHelpers.CD_SNOMED_40055000;
import static au.csiro.pathling.test.helpers.TerminologyHelpers.CD_SNOMED_403190006;
import static au.csiro.pathling.test.helpers.TerminologyHelpers.CD_SNOMED_444814009;
import static au.csiro.pathling.test.helpers.TerminologyHelpers.CD_SNOMED_900000000000003001;
import static au.csiro.pathling.test.helpers.TerminologyHelpers.HL7_USE_DISPLAY;
import static au.csiro.pathling.test.helpers.TerminologyHelpers.mockCoding;
import static au.csiro.pathling.test.helpers.TerminologyServiceHelpers.setupSubsumes;
import static org.junit.jupiter.api.Assertions.assertEquals;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.element.BooleanPath;
import au.csiro.pathling.fhirpath.element.DatePath;
import au.csiro.pathling.fhirpath.element.DecimalPath;
import au.csiro.pathling.fhirpath.element.IntegerPath;
import au.csiro.pathling.fhirpath.element.StringPath;
import au.csiro.pathling.fhirpath.literal.CodingLiteralPath;
import au.csiro.pathling.fhirpath.literal.DateLiteralPath;
import au.csiro.pathling.fhirpath.literal.DateTimeLiteralPath;
import au.csiro.pathling.fhirpath.literal.TimeLiteralPath;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.builders.ParserContextBuilder;
import au.csiro.pathling.test.helpers.TerminologyServiceHelpers;
import au.csiro.pathling.test.helpers.TerminologyServiceHelpers.TranslateExpectations;
import jakarta.annotation.Nonnull;
import java.util.Collections;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author Piotr Szul
 */
public class ParserTest extends AbstractParserTest {

  @SuppressWarnings("SameParameterValue")
  private <T extends Throwable> T assertThrows(final Class<T> errorType, final String expression) {
    return Assertions.assertThrows(errorType, () -> parser.parse(expression));
  }

  private TranslateExpectations setupMockTranslationFor_195662009_444814009(
      final String conceptMapUrl) {
    return TerminologyServiceHelpers.setupTranslate(terminologyService)
        .withMockTranslations(CD_SNOMED_195662009,
            conceptMapUrl, "uuid:test-system", 3)
        .withMockTranslations(CD_SNOMED_444814009,
            conceptMapUrl, "uuid:test-system", 2);
  }

  private void setupMockTranslationFor_195662009_444814009() {
    setupMockTranslationFor_195662009_444814009(
        "http://snomed.info/sct?fhir_cm=900000000000526001");
  }

  private void setupMockDisplayFor_195662009_444814009() {
    TerminologyServiceHelpers.setupLookup(terminologyService)
        .withDisplay(CD_SNOMED_195662009)
        .withDisplay(CD_SNOMED_444814009);
  }

  private void setupMockPropertiesFor_195662009_444814009() {
    TerminologyServiceHelpers.setupLookup(terminologyService)
        .withProperty(CD_SNOMED_195662009, "child", (String) null, CD_SNOMED_40055000,
            CD_SNOMED_403190006)
        .withProperty(CD_SNOMED_444814009, "child", (String) null, CD_SNOMED_284551006)
        .withDesignation(CD_SNOMED_195662009, CD_SNOMED_900000000000003001, "en",
            "Acute viral pharyngitis : disorder")
        .withDesignation(CD_SNOMED_444814009, CD_SNOMED_900000000000003001, "en",
            "Viral sinusitis : disorder")
        .withDesignation(CD_SNOMED_195662009, HL7_USE_DISPLAY, "en",
            "Acute viral pharyngitis")
        .withDesignation(CD_SNOMED_444814009, HL7_USE_DISPLAY, "en",
            "Viral sinusitis")
        .withDesignation(CD_SNOMED_444814009, HL7_USE_DISPLAY, "pl",
            "Wirusowe zapalenie zatok")
        .done();
  }

  @Test
  void testContainsOperator() {
    assertThatResultOf("name.family contains 'Wuckert783'")
        .isElementPath(BooleanPath.class)
        .selectOrderedResult()
        .hasRows(allPatientsWithValue(spark, false)
            .changeValue(PATIENT_ID_9360820c, true));

    assertThatResultOf("name.suffix contains 'MD'")
        .isElementPath(BooleanPath.class)
        .selectOrderedResult()
        .hasRows(allPatientsWithValue(spark, false)
            .changeValue(PATIENT_ID_8ee183e2, true));
  }

  @Test
  void testInOperator() {
    assertThatResultOf("'Wuckert783' in name.family")
        .isElementPath(BooleanPath.class)
        .selectOrderedResult()
        .hasRows(allPatientsWithValue(spark, false)
            .changeValue(PATIENT_ID_9360820c, true));

    assertThatResultOf("'MD' in name.suffix")
        .isElementPath(BooleanPath.class)
        .selectOrderedResult()
        .hasRows(allPatientsWithValue(spark, false)
            .changeValue(PATIENT_ID_8ee183e2, true));
  }

  @Test
  void testCodingOperations() {
    // Check that membership operators for codings use strict equality rather than equivalence.
    // test unversioned
    assertThatResultOf(
        "maritalStatus.coding contains http://terminology.hl7.org/CodeSystem/v3-MaritalStatus|S")
        .isElementPath(BooleanPath.class)
        .selectOrderedResult()
        .hasRows(allPatientsWithValue(spark, false));

    // test versioned
    assertThatResultOf(
        "http://terminology.hl7.org/CodeSystem/v2-0203|v2.0.3|PPN in identifier.type.coding")
        .isElementPath(BooleanPath.class)
        .selectOrderedResult()
        .hasRows(allPatientsWithValue(spark, false));
  }

  @Test
  void testDateTimeLiterals() {
    // Milliseconds precision.
    assertThatResultOf("@2015-02-04T14:34:28.350Z")
        .isLiteralPath(DateTimeLiteralPath.class)
        .hasExpression("@2015-02-04T14:34:28.350Z")
        .has("2015-02-04T14:34:28.350Z",
            dateTime -> dateTime.getValue().dateTimeValue().getValueAsString());

    // Milliseconds precision, no timezone.
    assertThatResultOf("@2015-02-04T14:34:28.350")
        .isLiteralPath(DateTimeLiteralPath.class)
        .hasExpression("@2015-02-04T14:34:28.350")
        .has("2015-02-04T14:34:28.350",
            dateTime -> dateTime.getValue().dateTimeValue().getValueAsString())
        .has(null, dateTime -> dateTime.getValue().dateTimeValue().getTimeZone());

    // Seconds precision.
    assertThatResultOf("@2015-02-04T14:34:28-05:00")
        .isLiteralPath(DateTimeLiteralPath.class)
        .hasExpression("@2015-02-04T14:34:28-05:00")
        .has("2015-02-04T14:34:28-05:00",
            dateTime -> dateTime.getValue().dateTimeValue().getValueAsString());

    // Seconds precision, no timezone.
    assertThatResultOf("@2015-02-04T14:34:28")
        .isLiteralPath(DateTimeLiteralPath.class)
        .hasExpression("@2015-02-04T14:34:28")
        .has("2015-02-04T14:34:28",
            dateTime -> dateTime.getValue().dateTimeValue().getValueAsString())
        .has(null, dateTime -> dateTime.getValue().dateTimeValue().getTimeZone());
  }

  @Test
  void testDateLiterals() {
    // Year, month and day.
    assertThatResultOf("@2015-02-04")
        .isLiteralPath(DateLiteralPath.class)
        .hasExpression("@2015-02-04")
        .has("2015-02-04", date -> date.getValue().castToDate(date.getValue()).getValueAsString())
        .has(null, date -> date.getValue().castToDate(date.getValue()).getTimeZone());

    // Year and month.
    assertThatResultOf("@2015-02")
        .isLiteralPath(DateLiteralPath.class)
        .hasExpression("@2015-02")
        .has("2015-02", date -> date.getValue().castToDate(date.getValue()).getValueAsString())
        .has(null, date -> date.getValue().castToDate(date.getValue()).getTimeZone());

    // Year only.
    assertThatResultOf("@2015")
        .isLiteralPath(DateLiteralPath.class)
        .hasExpression("@2015")
        .has("2015", date -> date.getValue().castToDate(date.getValue()).getValueAsString())
        .has(null, date -> date.getValue().castToDate(date.getValue()).getTimeZone());
  }

  @Test
  void testTimeLiterals() {
    // Hours, minutes and seconds.
    assertThatResultOf("@T14:34:28")
        .isLiteralPath(TimeLiteralPath.class)
        .hasExpression("@T14:34:28")
        .has("14:34:28", time -> time.getValue().castToTime(time.getValue()).getValueAsString());

    // Hours and minutes.
    assertThatResultOf("@T14:34")
        .isLiteralPath(TimeLiteralPath.class)
        .hasExpression("@T14:34")
        .has("14:34", time -> time.getValue().castToTime(time.getValue()).getValueAsString());

    // Hour only.
    assertThatResultOf("@T14")
        .isLiteralPath(TimeLiteralPath.class)
        .hasExpression("@T14")
        .has("14", time -> time.getValue().castToTime(time.getValue()).getValueAsString());
  }

  @Test
  void testCodingLiterals() {
    // Coding literal form [system]|[code]
    final Coding expectedCoding =
        new Coding("http://terminology.hl7.org/CodeSystem/v3-MaritalStatus", "S", null);
    assertThatResultOf("http://terminology.hl7.org/CodeSystem/v3-MaritalStatus|S")
        .isLiteralPath(CodingLiteralPath.class)
        .hasExpression("http://terminology.hl7.org/CodeSystem/v3-MaritalStatus|S")
        .hasCodingValue(expectedCoding);

    // Coding literal form [system]|[code]|[version]
    final Coding expectedCodingWithVersion =
        new Coding("http://terminology.hl7.org/CodeSystem/v3-MaritalStatus", "S", null);
    expectedCodingWithVersion.setVersion("v1");
    assertThatResultOf("http://terminology.hl7.org/CodeSystem/v3-MaritalStatus|S|v1")
        .isLiteralPath(CodingLiteralPath.class)
        .hasExpression("http://terminology.hl7.org/CodeSystem/v3-MaritalStatus|S|v1")
        .hasCodingValue(expectedCodingWithVersion);
  }

  @Test
  void testCountWithReverseResolve() {
    assertThatResultOf("reverseResolve(Condition.subject).code.coding.count()")
        .isElementPath(IntegerPath.class)
        .isSingular()
        .selectOrderedResult()
        .hasRows(
            allPatientsWithValue(spark, 8L)
                .changeValue(PATIENT_ID_121503c8, 10L)
                .changeValue(PATIENT_ID_2b36c1e2, 3L).changeValue(PATIENT_ID_7001ad9c, 5L)
                .changeValue(PATIENT_ID_9360820c, 16L).changeValue(PATIENT_ID_beff242e, 3L)
                .changeValue(PATIENT_ID_bbd33563, 10L));
  }

  @Test
  void testCount() {
    final DatasetBuilder expectedCountResult =
        allPatientsWithValue(spark, 1L)
            .changeValue(PATIENT_ID_9360820c, 2L);
    assertThatResultOf("name.count()")
        .selectOrderedResult()
        .hasRows(expectedCountResult);

    assertThatResultOf("name.family.count()")
        .selectOrderedResult()
        .hasRows(expectedCountResult);

    assertThatResultOf("name.given.count()")
        .selectOrderedResult()
        .hasRows(expectedCountResult);

    assertThatResultOf("name.prefix.count()")
        .selectOrderedResult()
        .hasRows(expectedCountResult.changeValue(PATIENT_ID_bbd33563, 0L));
  }

  @Test
  void testSubsumesAndSubsumedBy() {

    setupSubsumes(terminologyService);
    // Viral sinusitis (disorder) = http://snomed.info/sct|444814009 not in (PATIENT_ID_2b36c1e2,
    // PATIENT_ID_bbd33563, PATIENT_ID_7001ad9c)
    // Chronic sinusitis (disorder) = http://snomed.info/sct|40055000 in (PATIENT_ID_7001ad9c)

    // With empty concept map subsume should work as member of
    assertThatResultOf(
        "reverseResolve(Condition.subject).code.subsumes(http://snomed.info/sct|40055000)")
        .isElementPath(BooleanPath.class)
        .selectOrderedResult()
        .hasRows(spark, "responses/ParserTest/testSubsumesAndSubsumedBy-subsumes-empty.csv");

    assertThatResultOf(
        "reverseResolve(Condition.subject).code.subsumedBy(http://snomed.info/sct|40055000)")
        .isElementPath(BooleanPath.class)
        .selectOrderedResult()
        .hasRows(spark, "responses/ParserTest/testSubsumesAndSubsumedBy-subsumedBy-empty.csv");

    // on the same collection should return all True (even though one is CodeableConcept)
    assertThatResultOf(
        "reverseResolve(Condition.subject).code.coding.subsumes(%resource.reverseResolve(Condition.subject).code)")
        .selectOrderedResult()
        .hasRows(spark, "responses/ParserTest/testSubsumesAndSubsumedBy-subsumes-self.csv");

    setupSubsumes(terminologyService).withSubsumes(
        CD_SNOMED_444814009, CD_SNOMED_40055000);
    assertThatResultOf(
        "reverseResolve(Condition.subject).code.subsumes(http://snomed.info/sct|40055000)")
        .selectOrderedResult()
        .hasRows(spark, "responses/ParserTest/testSubsumesAndSubsumedBy-subsumes.csv");

    assertThatResultOf("reverseResolve(Condition.subject).code.subsumedBy"
        + "(http://snomed.info/sct|40055000|http://snomed.info/sct/32506021000036107/version/20200229)")
        .selectOrderedResult()
        .hasRows(spark, "responses/ParserTest/testSubsumesAndSubsumedBy-subsumedBy.csv");
  }

  @Test
  void testWhereWithAggregateFunction() {
    assertThatResultOf("where($this.name.given.first() = 'Karina848').gender")
        .selectOrderedResult()
        .hasRows(allPatientsWithValue(spark, DataTypes.StringType, null)
            .changeValue(PATIENT_ID_9360820c, "female"));
  }

  /**
   * This tests that the value from the `$this` context gets preserved successfully, when used in
   * the "element" operand to the membership operator.
   */
  @Test
  void testWhereWithContainsOperator() {
    assertThatResultOf("where($this.name.given contains 'Karina848').gender")
        .selectOrderedResult()
        .hasRows(allPatientsWithValue(spark, (String) null)
            .changeValue(PATIENT_ID_9360820c, "female"));
  }

  @Test
  void testWhereWithSubsumes() {

    setupSubsumes(terminologyService).withSubsumes(CD_SNOMED_284551006, CD_SNOMED_40055000);

    assertThatResultOf(
        "where($this.reverseResolve(Condition.subject).code"
            + ".subsumedBy(http://snomed.info/sct|284551006) contains true).gender")
        .selectOrderedResult()
        .hasRows(allPatientsWithValue(spark, (String) null)
            .changeValue(PATIENT_ID_7001ad9c, "female") // has code 40055000
            .changeValue(PATIENT_ID_bbd33563, "male")  // has code 284551006
        );
  }

  @Test
  void testWhereWithMemberOf() {

    TerminologyServiceHelpers.setupValidate(terminologyService)
        .withValueSet("http://snomed.info/sct?fhir_vs=refset/32570521000036109",
            CD_SNOMED_403190006, CD_SNOMED_284551006);
    assertThatResultOf(
        "reverseResolve(Condition.subject).where("
            + "$this.code.memberOf('http://snomed.info/sct?fhir_vs=refset/32570521000036109'))"
            + ".recordedDate")
        .selectOrderedResult()
        .hasRows(spark, "responses/ParserTest/testWhereWithMemberOf.csv");
  }

  /**
   * This tests that the value from the `$this` context gets preserved successfully, when used in
   * the "collection" operand to the membership operator. It also tests that aggregation can be
   * applied successfully following a nested where invocation.
   */
  @Test
  void testAggregationFollowingNestedWhere() {
    assertThatResultOf(
        "where(name.where(use = 'official').first().given.first() in "
            + "name.where(use = 'maiden').first().given).gender")
        .selectOrderedResult()
        .hasRows(spark, "responses/ParserTest/testAggregationFollowingNestedWhere.csv");
  }

  @Test
  void testNestedWhereWithAggregationOnElement() {
    assertThatResultOf(
        "name.where('Karina848' in where(use contains 'maiden').given).family")
        .selectOrderedResult()
        .hasRows(spark, "responses/ParserTest/testNestedWhereWithAggregationOnElement.csv");
  }

  @Test
  void testBooleanOperatorWithTwoLiterals() {
    assertThatResultOf("true and false")
        .selectOrderedResult();
  }

  @Test
  void testQueryWithExternalConstantInWhere() {
    assertThatResultOf(
        "name.family.where($this = %resource.name.family.first())")
        .selectOrderedResult()
        .hasRows(spark, "responses/ParserTest/testQueryWithExternalConstantInWhere.csv");

    assertThatResultOf(
        "name.family.where($this = %context.name.family.first())")
        .selectOrderedResult()
        .hasRows(spark, "responses/ParserTest/testQueryWithExternalConstantInWhere.csv");

    assertThatResultOf(
        "name.family.where(%resource.name.family.first() = $this)")
        .selectOrderedResult()
        .hasRows(spark, "responses/ParserTest/testQueryWithExternalConstantInWhere.csv");
  }

  @Test
  void testExternalConstantHasCorrectExpression() {
    assertThatResultOf("%resource")
        .hasExpression("%resource");

    assertThatResultOf("%context")
        .hasExpression("%context");
  }

  @Test
  void testNotFunction() {
    assertThatResultOf(
        "(name.given contains 'Su690').not()")
        .selectOrderedResult()
        .hasRows(spark, "responses/ParserTest/testNotFunction.csv");
  }

  @Test
  void testIfFunction() {
    assertThatResultOf(
        "gender.iif($this = 'male', 'Male', 'Not male')")
        .selectOrderedResult()
        .hasRows(spark, "responses/ParserTest/testIfFunction.csv");
  }

  @Test
  void testIfFunctionWithComplexTypeResult() {
    assertThatResultOf(
        "iif(gender = 'male', contact.name, name).given")
        .selectOrderedResult()
        .hasRows(spark, "responses/ParserTest/testIfFunctionWithComplexTypeResult.csv");
  }

  @Test
  void testIfFunctionWithUntypedResourceResult() {
    assertThatResultOf(
        "iif(gender = 'male', link.where(type = 'replaced-by').other.resolve(), "
            + "link.where(type = 'replaces').other.resolve()).ofType(Patient).gender")
        .selectOrderedResult()
        .hasRows(allPatientsWithValue(spark, (String) null));
  }

  @Test
  void testIfFunctionWithResourceResult() {
    assertThatResultOf(
        "iif(gender = 'male', contact.where(gender = 'male').organization.resolve(), "
            + "contact.where(gender = 'female').organization.resolve()).name")
        .selectOrderedResult()
        .hasRows(allPatientsWithValue(spark, (String) null));
  }

  @Test
  void testTranslateFunction() {

    setupMockTranslationFor_195662009_444814009();

    assertThatResultOf(ResourceType.CONDITION,
        "code.coding.translate('http://snomed.info/sct?fhir_cm=900000000000526001', false, 'equivalent').code")
        .selectOrderedResult()
        .hasRows(spark, "responses/ParserTest/testTranslateFunction.csv");
  }

  @Test
  void testDisplayFunction() {
    setupMockDisplayFor_195662009_444814009();
    assertThatResultOf(ResourceType.CONDITION,
        "code.coding.display()")
        .selectOrderedResult()
        .hasRows(spark, "responses/ParserTest/testDisplayFunction.csv");
  }


  @Test
  void testPropertyFunctionWithDefaultType() {
    setupMockDisplayFor_195662009_444814009();
    assertThatResultOf(ResourceType.CONDITION,
        "code.coding.property('display')")
        .selectOrderedResult()
        .hasRows(spark, "responses/ParserTest/testDisplayFunction.csv");
  }

  @Test
  void testPropertyFunctionWithCodingType() {
    setupMockPropertiesFor_195662009_444814009();
    assertThatResultOf(ResourceType.CONDITION,
        "code.coding.property('child', 'Coding').code")
        .selectOrderedResult()
        .hasRows(spark, "responses/ParserTest/testPropertyFunctionWithCodingType.csv");
  }


  @Test
  void testDesignationFunctionWithLanguage() {

    setupMockPropertiesFor_195662009_444814009();
    assertThatResultOf(ResourceType.CONDITION,
        "code.coding.designation(http://snomed.info/sct|900000000000003001, 'en')")
        .selectOrderedResult()
        .hasRows(spark, "responses/ParserTest/testDesignationFunctionWithLanguage.csv");
  }

  @Test
  void testDesignationFunctionWithNoLanguage() {

    setupMockPropertiesFor_195662009_444814009();
    assertThatResultOf(ResourceType.CONDITION,
        "code.coding.designation(http://terminology.hl7.org/CodeSystem/designation-usage|display)")
        .selectOrderedResult()
        .hasRows(spark, "responses/ParserTest/testDesignationFunctionWithNoLanguage.csv");
  }

  @Test
  void testTranslateWithWhereAndTranslate() {

    setupMockTranslationFor_195662009_444814009("uuid:cm=1")
        .withMockTranslations(mockCoding("uuid:test-system", "444814009", 0), "uuid:cm=2",
            "uuid:other-system", 1)
        .withMockTranslations(mockCoding("uuid:test-system", "444814009", 1), "uuid:cm=2",
            "uuid:other-system", 2);

    assertThatResultOf(ResourceType.CONDITION,
        "code.translate('uuid:cm=1', false, 'equivalent').where($this.translate('uuid:cm=2', false, 'equivalent').code.count()=13).code")
        .selectOrderedResult()
        .hasRows(spark, "responses/ParserTest/testTranslateWithWhereAndTranslate.csv");
  }

  @Test
  void testWithCodingLiteral() {
    assertThatResultOf(
        "maritalStatus.coding contains http://terminology.hl7.org/CodeSystem/v3-MaritalStatus|S||S")
        .selectOrderedResult()
        .hasRows(spark, "responses/ParserTest/testWithCodingLiteral.csv");
  }

  @Test
  void testCombineOperator() {
    assertThatResultOf("name.family combine name.given")
        .isElementPath(StringPath.class)
        .selectResult()
        .hasRows(spark, "responses/ParserTest/testCombineOperator.csv");
  }

  @Test
  void testCombineOperatorWithWhereFunction() {
    assertThatResultOf("where((name.family combine name.given) contains 'Gleichner915').birthDate")
        .isElementPath(DatePath.class)
        .selectResult()
        .hasRows(spark, "responses/ParserTest/testCombineOperatorWithWhereFunction.csv");
  }

  @Test
  void testCombineOperatorWithResourcePaths() {
    assertThatResultOf(
        "reverseResolve(Condition.subject).where(clinicalStatus.coding.code contains 'active') combine reverseResolve(Condition.subject).where(clinicalStatus.coding.code contains 'resolved')")
        .isResourcePath()
        .selectResult()
        .hasRows(spark, "responses/ParserTest/testCombineOperatorWithResourcePaths.csv");
  }

  @Test
  void testCombineOperatorWithDifferentlyTypedStringPaths() {
    assertThatResultOf(
        "reverseResolve(Condition.subject).code.coding.system combine "
            + "reverseResolve(Condition.subject).code.coding.code")
        .isElementPath(StringPath.class)
        .selectResult()
        .hasRows(spark,
            "responses/ParserTest/testCombineOperatorWithDifferentlyTypedStringPaths.csv");
  }

  @Test
  void testCombineOperatorWithComplexTypeAndNull() {
    assertThatResultOf("(name combine {}).given")
        .isElementPath(StringPath.class)
        .selectResult()
        .hasRows(spark,
            "responses/ParserTest/testCombineOperatorWithComplexTypeAndNull.csv");
  }

  @Test
  void testCombineOperatorWithTwoLiterals() {
    assertThatResultOf("1 combine 2")
        .isElementPath(IntegerPath.class)
        .selectResult()
        .hasRows(spark, "responses/ParserTest/testCombineOperatorWithTwoLiterals.csv");
  }

  @Test
  void testCombineOperatorWithTwoUntypedResourcePaths() {
    assertThatResultOf(
        "(reverseResolve(Condition.subject).subject.resolve() combine "
            + "reverseResolve(DiagnosticReport.subject).subject.resolve()).ofType(Patient)")
        .isResourcePath()
        .hasResourceType(ResourceType.PATIENT)
        .selectResult()
        .hasRows(spark, "responses/ParserTest/testCombineOperatorWithTwoUntypedResourcePaths.csv");
  }

  @Test
  void testCombineOperatorWithCodingLiterals() {
    assertThatResultOf(
        "(http://snomed.info/sct|410429000||'Cardiac Arrest' combine "
            + "http://snomed.info/sct|230690007||'Stroke').empty()")
        .isElementPath(BooleanPath.class)
        .selectResult()
        .hasRows(spark, "responses/ParserTest/testCombineOperatorWithCodingLiterals.csv");
  }

  @Test
  void testBooleanOperatorWithLeftLiteral() {
    assertThatResultOf("@1970-11-22 = birthDate")
        .isElementPath(BooleanPath.class)
        .selectResult()
        .hasRows(spark, "responses/ParserTest/testBooleanOperatorWithLeftLiteral.csv");
  }

  @Test
  void parserErrorThrows() {
    final InvalidUserInputError error = assertThrows(InvalidUserInputError.class,
        "(reasonCode.coding.display contains 'Viral pneumonia') and (class.code = 'AMB'");
    assertEquals(
        "Error parsing FHIRPath expression (line: 1, position: 78): missing ')' at '<EOF>'",
        error.getMessage());
  }


  @Test
  void testExtensionsOnResources() {
    assertThatResultOf(
        "extension.url")
        .isElementPath(StringPath.class)
        .selectResult()
        .hasRows(spark, "responses/ParserTest/testExtensionsOnResources.csv");
  }

  @Test
  void testExtensionFunction() {
    // This should be the same as: "extension.where($this.url='http://hl7.org/fhir/StructureDefinition/patient-mothersMaidenName').valueString"
    assertThatResultOf(
        "extension('http://hl7.org/fhir/StructureDefinition/patient-mothersMaidenName').valueString")
        .isElementPath(StringPath.class)
        .selectResult()
        .hasRows(spark, "responses/ParserTest/testExtensionFunction.csv");
  }

  @Test
  void testExtensionsOnElements() {
    assertThatResultOf(
        "address.extension.url")
        .isElementPath(StringPath.class)
        .selectResult()
        .hasRows(spark, "responses/ParserTest/testExtensionsOnElements.csv");
  }

  @Test
  void testNestedExtensions() {
    assertThatResultOf(
        "extension.extension.url")
        .isElementPath(StringPath.class)
        .selectResult()
        .hasRows(spark, "responses/ParserTest/testNestedExtensions.csv");
  }

  @Test
  void testExtensionsCurrentResource() {
    assertThatResultOf(ResourceType.CONDITION,
        "subject.resolve().ofType(Patient).extension.url")
        .isElementPath(StringPath.class)
        .selectResult()
        .hasRows(spark, "responses/ParserTest/testExtensionsCurrentResource.csv");
  }

  @Test
  void testComplexExtensionsOnComplexPath() {
    assertThatResultOf(
        "address.where($this.city = 'Boston')"
            + ".extension('http://hl7.org/fhir/StructureDefinition/geolocation')"
            + ".extension('latitude').valueDecimal")
        .isElementPath(DecimalPath.class)
        .selectResult()
        .hasRows(spark, "responses/ParserTest/testComplexExtensionsOnComplexPath.csv");
  }

  @Test
  void testExtensionFunctionInWhere() {
    assertThatResultOf(
        "address.where($this.extension('http://hl7.org/fhir/StructureDefinition/geolocation').extension('latitude').valueDecimal contains 42.391383).city")
        .isElementPath(StringPath.class)
        .selectResult()
        .hasRows(spark, "responses/ParserTest/testExtensionFunctionInWhere.csv");
  }


  @Test
  void testExtensionFunctionOnTranslateResult() {

    // This is a special case as the codings here are created from the terminology server response
    // using the hardcoded encoding core in CodingEncoding.

    setupMockTranslationFor_195662009_444814009();

    assertThatResultOf(ResourceType.CONDITION,
        "code.coding.translate('http://snomed.info/sct?fhir_cm=900000000000526001', false, 'equivalent').extension('uuid:any').url")
        .selectOrderedResult()
        .hasRows(spark, "responses/ParserTest/testExtensionFunctionOnTranslateResult.csv");
  }

  @Test
  void testTraversalIntoMissingOpenType() {
    final String expression = "extension('http://hl7.org/fhir/R4/extension-patient-birthplace.html').valueOid";
    final InvalidUserInputError error = assertThrows(InvalidUserInputError.class,
        expression);
    assertEquals("No such child: " + expression, error.getMessage());
  }

  @Test
  void testReverseResolveFollowingMonomorphicResolve() {
    setSubjectResource(ResourceType.ENCOUNTER);
    assertThatResultOf(
        "serviceProvider.resolve().reverseResolve(Encounter.serviceProvider).id")
        .isElementPath(StringPath.class)
        .selectResult()
        .hasRows(spark, "responses/ParserTest/testReverseResolveFollowingMonomorphicResolve.csv");
  }

  @Test
  void testReverseResolveFollowingPolymorphicResolve() {
    setSubjectResource(ResourceType.ENCOUNTER);
    assertThatResultOf(
        "subject.resolve().ofType(Patient).reverseResolve(Encounter.subject).id "
            + "contains '2aff9edd-def2-487a-b435-a162e11a303c'")
        .isElementPath(BooleanPath.class)
        .selectResult()
        .hasRows(spark, "responses/ParserTest/testReverseResolveFollowingPolymorphicResolve.csv");
  }

  @Test
  void testReverseResolveFollowingReverseResolve() {
    assertThatResultOf(
        "reverseResolve(Encounter.subject).reverseResolve(CarePlan.encounter).id")
        .isElementPath(StringPath.class)
        .selectResult()
        .hasRows(spark, "responses/ParserTest/testReverseResolveFollowingReverseResolve.csv");
  }

  @Test
  void testIifWithNullLiteral() {
    assertThatResultOf("iif(gender='male', birthDate, {})")
        .isElementPath(DatePath.class)
        .selectResult()
        .hasRows(spark, "responses/ParserTest/testIifWithNullLiteral.csv");
  }

  @Test
  void testUntilFunction() {
    setSubjectResource(ResourceType.ENCOUNTER);
    assertThatResultOf(
        "subject.resolve().ofType(Patient).birthDate.until(%resource.period.start, 'years')")
        .isElementPath(IntegerPath.class)
        .selectResult()
        .hasRows(spark, "responses/ParserTest/testUntilFunction.csv");
  }

  @SuppressWarnings("SameParameterValue")
  private void setSubjectResource(@Nonnull final ResourceType resourceType) {
    final ResourcePath subjectResource = ResourcePath
        .build(fhirContext, dataSource, resourceType, resourceType.toCode(),
            true);

    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext)
        .terminologyClientFactory(terminologyServiceFactory)
        .database(dataSource)
        .inputContext(subjectResource)
        .groupingColumns(Collections.singletonList(subjectResource.getIdColumn()))
        .build();
    parser = new Parser(parserContext);
  }

  @Test
  void testQuantityMultiplicationAndDivision() {
    assertThatResultOf(
        "((reverseResolve(Observation.subject).where(valueQuantity < 150 'cm').valueQuantity.first() * 2 '1')"
            + " / reverseResolve(Observation.subject).where(valueQuantity < 1.50 'm').valueQuantity.first()).value")
        .isElementPath(DecimalPath.class)
        .selectResult()
        .hasRows(spark, "responses/ParserTest/testQuantityMultiplicationAndDivision.csv");
  }

  @Test
  void testQuantityAdditionSubtractionAndEquality() {
    //  33 'mmol/L == 19873051110000000000000000 'm-3'
    assertThatResultOf(
        "((reverseResolve(Observation.subject).where(valueQuantity > 1 'mmol/L').valueQuantity.first() + 33 'mmol/L')"
            + " - reverseResolve(Observation.subject).where(valueQuantity > 1 'mmol/L').valueQuantity.first()) = 19873051110000000000000000 'm-3'")
        .isElementPath(BooleanPath.class)
        .selectResult()
        .hasRows(spark, "responses/ParserTest/testQuantityAdditionSubtractionAndEquality.csv");
  }

  @Test
  void testQuantityAdditionWithOverflow() {
    // values for 121503c8-9564-4b48-9086-a22df717948e and a7eb2ce7-1075-426c-addd-957b861b0e55 exceed 10^26 m-3
    assertThatResultOf(
        "(reverseResolve(Observation.subject).where(valueQuantity > 100 'mmol/L').valueQuantity.first() + 33 'mmol/L').value")
        .isElementPath(DecimalPath.class)
        .selectResult()
        .hasRows(spark, "responses/ParserTest/testQuantityAdditionWithOverflow_value.csv");
    assertThatResultOf(
        "(reverseResolve(Observation.subject).where(valueQuantity > 100 'mmol/L').valueQuantity.first() + 33 'mmol/L').code")
        .isElementPath(StringPath.class)
        .selectResult()
        .hasRows(spark, "responses/ParserTest/testQuantityAdditionWithOverflow_code.csv");
  }

  @Test
  void testResolutionOfExtensionReference() {
    mockResource(ResourceType.PATIENT, ResourceType.ENCOUNTER, ResourceType.GOAL);
    assertThatResultOf(
        "reverseResolve(Encounter.subject).extension.where(url = 'urn:test:associated-goal')"
            + ".valueReference.resolve().ofType(Goal).description.text")
        .isElementPath(StringPath.class)
        .selectResult()
        .hasRows(spark, "responses/ParserTest/testResolutionOfExtensionReference.csv");
  }

  @Test
  void testResolutionOfExtensionReferenceWithWrongType() {
    mockResource(ResourceType.PATIENT, ResourceType.ENCOUNTER, ResourceType.GOAL);
    assertThatResultOf(
        "reverseResolve(Encounter.subject).extension.where(url = 'urn:test:associated-goal')"
            + ".valueReference.resolve().ofType(Condition).id")
        .isElementPath(StringPath.class)
        .selectResult()
        .hasRows(spark, "responses/ParserTest/testResolutionOfExtensionReferenceWithWrongType.csv");
  }

}
