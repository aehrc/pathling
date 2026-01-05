/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.fhirpath.dsl;

import static au.csiro.pathling.test.yaml.FhirTypedLiteral.toCoding;
import static au.csiro.pathling.test.yaml.FhirTypedLiteral.toInteger;
import static org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType.CODEABLECONCEPT;

import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.test.SharedMocks;
import au.csiro.pathling.test.dsl.FhirPathDslTestBase;
import au.csiro.pathling.test.dsl.FhirPathTest;
import au.csiro.pathling.test.helpers.TerminologyServiceHelpers;
import java.util.List;
import java.util.stream.Stream;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.DateTimeType;
import org.hl7.fhir.r4.model.IntegerType;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.codesystems.ConceptMapEquivalence;
import org.junit.jupiter.api.DynamicTest;
import org.springframework.beans.factory.annotation.Autowired;

/** Tests for terminology functions. */
public class TerminologyFunctionsDslTest extends FhirPathDslTestBase {

  @Autowired TerminologyService terminologyService;

  @FhirPathTest
  public Stream<DynamicTest> testDisplayFunction() {
    // Reset mocks and setup terminology service expectations
    SharedMocks.resetAll();

    // Create codings for testing
    final Coding loincCoding =
        new Coding(
            "http://loinc.org",
            "55915-3",
            "Beta 2 globulin [Mass/volume] in Cerebral spinal fluid by Electrophoresis");
    final Coding snomedCoding =
        new Coding("http://snomed.info/sct", "63816008", "Left hepatectomy");
    final Coding weightCoding = new Coding("http://loinc.org", "29463-7", "Body weight");

    // Setup terminology service expectations
    TerminologyServiceHelpers.setupLookup(terminologyService)
        // Setup display values without language parameter
        .withDisplay(loincCoding)
        .withDisplay(snomedCoding)
        .withDisplay(weightCoding)

        // Setup display values with language parameter
        .withDisplay(loincCoding, "LC_55915_3 (DE)", "de")
        .withDisplay(snomedCoding, "CD_SNOMED_VER_63816008 (DE)", "de")
        .withDisplay(weightCoding, "LC_29463_7 (DE)", "de")
        .done();

    return builder()
        .withSubject(
            sb ->
                sb
                    // Empty coding
                    .coding("emptyCoding", null)
                    // Single codings
                    .coding(
                        "loinc",
                        "http://loinc.org|55915-3||'Beta 2 globulin [Mass/volume] in Cerebral"
                            + " spinal fluid by Electrophoresis'")
                    .coding("snomed", "http://snomed.info/sct|63816008||'Left hepatectomy'")
                    // Array of codings
                    .codingArray(
                        "multipleCodings",
                        "http://loinc.org|55915-3||'Beta 2 globulin [Mass/volume] in Cerebral"
                            + " spinal fluid by Electrophoresis'",
                        "http://snomed.info/sct|63816008||'Left hepatectomy'",
                        "http://loinc.org|29463-7||'Body weight'"))
        .group("display() function with no language parameter")
        .testEquals(
            "Beta 2 globulin [Mass/volume] in Cerebral spinal fluid by Electrophoresis",
            "loinc.display()",
            "display() returns the display value for a single LOINC coding")
        .testEquals(
            "Left hepatectomy",
            "snomed.display()",
            "display() returns the display value for a single SNOMED coding")
        .testEmpty("emptyCoding.display()", "display() returns empty for an empty coding")
        .testEquals(
            "Beta 2 globulin [Mass/volume] in Cerebral spinal fluid by Electrophoresis",
            "multipleCodings[0].display()",
            "display() returns the display value for the first coding in an array")
        .testEquals(
            "Left hepatectomy",
            "multipleCodings[1].display()",
            "display() returns the display value for the second coding in an array")
        .testEquals(
            "Body weight",
            "multipleCodings[2].display()",
            "display() returns the display value for the third coding in an array")
        .group("display() function with language parameter")
        .testEquals(
            "LC_55915_3 (DE)",
            "loinc.display('de')",
            "display() with language parameter returns the localized display value for LOINC")
        .testEquals(
            "CD_SNOMED_VER_63816008 (DE)",
            "snomed.display('de')",
            "display() with language parameter returns the localized display value for SNOMED")
        .testEmpty(
            "emptyCoding.display('de')",
            "display() with language parameter returns empty for an empty coding")
        .testEquals(
            "LC_55915_3 (DE)",
            "multipleCodings[0].display('de')",
            "display() with language parameter returns the localized display value for the first"
                + " coding in an array")
        .testEquals(
            "CD_SNOMED_VER_63816008 (DE)",
            "multipleCodings[1].display('de')",
            "display() with language parameter returns the localized display value for the second"
                + " coding in an array")
        .testEquals(
            "LC_29463_7 (DE)",
            "multipleCodings[2].display('de')",
            "display() with language parameter returns the localized display value for the third"
                + " coding in an array")
        .group("display() function on collections")
        .testEquals(
            List.of(
                "Beta 2 globulin [Mass/volume] in Cerebral spinal fluid by Electrophoresis",
                "Left hepatectomy",
                "Body weight"),
            "multipleCodings.display()",
            "display() returns display names on a collection of codings")
        .testEquals(
            List.of("LC_55915_3 (DE)", "CD_SNOMED_VER_63816008 (DE)", "LC_29463_7 (DE)"),
            "multipleCodings.display('de')",
            "display() returns localized display names on a collection of codings")
        .group("display() function error cases")
        .testError(
            "'string'.display()", "display() throws an error when called on a non-coding type")
        .testError(
            "loinc.display('en', 'fr')",
            "display() throws an error when called with more than one parameter")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testPropertyFunction() {
    // Reset mocks and setup terminology service expectations
    SharedMocks.resetAll();

    // Create codings for testing
    final Coding loincCoding =
        new Coding(
            "http://loinc.org",
            "55915-3",
            "Beta 2 globulin [Mass/volume] in Cerebral spinal fluid by Electrophoresis");
    final Coding snomedCoding =
        new Coding("http://snomed.info/sct", "63816008", "Left hepatectomy");

    // Setup terminology service expectations for properties
    TerminologyServiceHelpers.setupLookup(terminologyService)
        // Setup string properties
        .withProperty(loincCoding, "description", null, new StringType("LOINC description"))
        .withProperty(snomedCoding, "description", null, new StringType("SNOMED description"))

        // Setup code properties
        .withProperty(loincCoding, "parent", null, new CodeType("55900-0"))
        .withProperty(snomedCoding, "parent", null, new CodeType("107963000"))

        // Setup integer properties
        .withProperty(loincCoding, "rank", null, new IntegerType(5))
        .withProperty(snomedCoding, "rank", null, new IntegerType(10))

        // Setup boolean properties
        .withProperty(loincCoding, "active", null, new BooleanType(true))
        .withProperty(snomedCoding, "active", null, new BooleanType(false))

        // Setup DateTime properties
        .withProperty(loincCoding, "effectiveDate", null, new DateTimeType("2020-01-01"))
        .withProperty(snomedCoding, "effectiveDate", null, new DateTimeType("2019-06-15"))

        // Setup Coding properties
        .withProperty(
            loincCoding,
            "category",
            null,
            new Coding("http://loinc.org/category", "LAB", "Laboratory"))
        .withProperty(
            snomedCoding,
            "category",
            null,
            new Coding("http://snomed.info/sct/category", "PROC", "Procedure"))

        // Setup properties with language parameter
        .withProperty(loincCoding, "description", "de", new StringType("LOINC Beschreibung"))
        .withProperty(snomedCoding, "description", "de", new StringType("SNOMED Beschreibung"))

        // Setup multiple property values
        .withProperty(
            loincCoding,
            "synonym",
            null,
            new StringType("Beta-2 globulin"),
            new StringType("β2 globulin"))
        .withProperty(
            snomedCoding,
            "synonym",
            null,
            new StringType("Left hepatic lobectomy"),
            new StringType("Left liver resection"))
        .done();

    return builder()
        .withSubject(
            sb ->
                sb
                    // Empty coding
                    .coding("emptyCoding", null)
                    // Single codings
                    .coding(
                        "loinc",
                        "http://loinc.org|55915-3||'Beta 2 globulin [Mass/volume] in Cerebral"
                            + " spinal fluid by Electrophoresis'")
                    .coding("snomed", "http://snomed.info/sct|63816008||'Left hepatectomy'")
                    // Array of codings
                    .codingArray(
                        "multipleCodings",
                        "http://loinc.org|55915-3||'Beta 2 globulin [Mass/volume] in Cerebral"
                            + " spinal fluid by Electrophoresis'",
                        "http://snomed.info/sct|63816008||'Left hepatectomy'"))
        .group("property() function with string type (default)")
        .testEquals(
            "LOINC description",
            "loinc.property('description')",
            "property() returns string property for a single LOINC coding")
        .testEquals(
            "SNOMED description",
            "snomed.property('description')",
            "property() returns string property for a single SNOMED coding")
        .testEmpty(
            "emptyCoding.property('description')", "property() returns empty for an empty coding")
        .testEquals(
            List.of("Beta-2 globulin", "β2 globulin"),
            "loinc.property('synonym')",
            "property() returns multiple string values when available")
        .group("property() function with explicit type parameters")
        .testEquals(
            "55900-0",
            "loinc.property('parent', 'code')",
            "property() returns code property for a LOINC coding")
        .testEquals(
            "107963000",
            "snomed.property('parent', 'code')",
            "property() returns code property for a SNOMED coding")
        .testEquals(
            toInteger("5"),
            "loinc.property('rank', 'integer')",
            "property() returns integer property for a LOINC coding")
        .testEquals(
            10,
            "snomed.property('rank', 'integer')",
            "property() returns integer property for a SNOMED coding")
        .testEquals(
            true,
            "loinc.property('active', 'boolean')",
            "property() returns boolean property for a LOINC coding")
        .testEquals(
            false,
            "snomed.property('active', 'boolean')",
            "property() returns boolean property for a SNOMED coding")
        .testEquals(
            "2020-01-01",
            "loinc.property('effectiveDate', 'dateTime')",
            "property() returns dateTime property for a LOINC coding")
        .testEquals(
            "2019-06-15",
            "snomed.property('effectiveDate', 'dateTime')",
            "property() returns dateTime property for a SNOMED coding")
        .group("property() function with Coding type")
        .testEquals(
            toCoding("http://loinc.org/category|LAB||'Laboratory'"),
            "loinc.property('category', 'Coding')",
            "property() returns Coding property for a LOINC coding")
        .testEquals(
            toCoding("http://snomed.info/sct/category|PROC||'Procedure'"),
            "snomed.property('category', 'Coding')",
            "property() returns Coding property for a SNOMED coding")
        .group("property() function with language parameter")
        .testEquals(
            "LOINC Beschreibung",
            "loinc.property('description', 'string', 'de')",
            "property() with language parameter returns localized string property for LOINC")
        .testEquals(
            "SNOMED Beschreibung",
            "snomed.property('description', 'string', 'de')",
            "property() with language parameter returns localized string property for SNOMED")
        .group("property() function on collections")
        .testEquals(
            List.of("LOINC description", "SNOMED description"),
            "multipleCodings.property('description')",
            "property() returns string properties on a collection of codings")
        .testEquals(
            List.of("55900-0", "107963000"),
            "multipleCodings.property('parent', 'code')",
            "property() returns code properties on a collection of codings")
        .testEquals(
            List.of("LOINC Beschreibung", "SNOMED Beschreibung"),
            "multipleCodings.property('description', 'string', 'de')",
            "property() with language parameter returns localized string properties on a collection"
                + " of codings")
        .group("property() function error cases")
        .testError(
            "'string'.property('description')",
            "property() throws an error when called on a non-coding type")
        .testError(
            "loinc.property('description', 'invalidType')",
            "property() throws an error when called with an invalid type parameter")
        .testError("loinc.property()", "property() throws an error when called with no parameters")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testMemberOfFunction() {
    // Reset mocks and setup terminology service expectations
    SharedMocks.resetAll();

    // Create codings for testing
    final Coding loincCoding =
        new Coding(
            "http://loinc.org",
            "55915-3",
            "Beta 2 globulin [Mass/volume] in Cerebral spinal fluid by Electrophoresis");
    final Coding snomedCoding =
        new Coding("http://snomed.info/sct", "63816008", "Left hepatectomy");
    final Coding weightCoding = new Coding("http://loinc.org", "29463-7", "Body weight");

    // Define value set URLs
    final String labTestsValueSet = "http://example.org/fhir/ValueSet/lab-tests";
    final String proceduresValueSet = "http://example.org/fhir/ValueSet/procedures";
    final String vitalSignsValueSet = "http://example.org/fhir/ValueSet/vital-signs";

    // Setup terminology service expectations for memberOf
    TerminologyServiceHelpers.setupValidate(terminologyService)
        // Lab tests value set includes LOINC coding but not SNOMED
        .withValueSet(labTestsValueSet, loincCoding)
        // Procedures value set includes SNOMED coding but not LOINC
        .withValueSet(proceduresValueSet, snomedCoding)
        // Vital signs value set includes weight coding
        .withValueSet(vitalSignsValueSet, weightCoding);

    return builder()
        .withSubject(
            sb ->
                sb
                    // Empty coding
                    .coding("emptyCoding", null)
                    // Single codings
                    .coding(
                        "loinc",
                        "http://loinc.org|55915-3||'Beta 2 globulin [Mass/volume] in Cerebral"
                            + " spinal fluid by Electrophoresis'")
                    .coding("snomed", "http://snomed.info/sct|63816008||'Left hepatectomy'")
                    .coding("weight", "http://loinc.org|29463-7||'Body weight'")
                    // Array of codings
                    .codingArray(
                        "multipleCodings",
                        "http://loinc.org|55915-3||'Beta 2 globulin [Mass/volume] in Cerebral"
                            + " spinal fluid by Electrophoresis'",
                        "http://snomed.info/sct|63816008||'Left hepatectomy'",
                        "http://loinc.org|29463-7||'Body weight'")
                    // CodeableConcepts with single coding
                    .element(
                        "loincConcept",
                        concept ->
                            concept
                                .fhirType(CODEABLECONCEPT)
                                .codingArray(
                                    "coding",
                                    "http://loinc.org|55915-3||'Beta 2 globulin [Mass/volume] in"
                                        + " Cerebral spinal fluid by Electrophoresis'"))
                    .element(
                        "snomedConcept",
                        concept ->
                            concept
                                .fhirType(CODEABLECONCEPT)
                                .codingArray(
                                    "coding",
                                    "http://snomed.info/sct|63816008||'Left hepatectomy'"))
                    // CodeableConcept with multiple codings
                    .element(
                        "mixedConcept",
                        concept ->
                            concept
                                .fhirType(CODEABLECONCEPT)
                                .codingArray(
                                    "coding",
                                    "http://loinc.org|55915-3||'Beta 2 globulin [Mass/volume] in"
                                        + " Cerebral spinal fluid by Electrophoresis'",
                                    "http://snomed.info/sct|63816008||'Left hepatectomy'"))
                    // Array of CodeableConcepts
                    .elementArray(
                        "multipleConcepts",
                        concept1 ->
                            concept1
                                .fhirType(CODEABLECONCEPT)
                                .codingArray(
                                    "coding",
                                    "http://loinc.org|55915-3||'Beta 2 globulin [Mass/volume] in"
                                        + " Cerebral spinal fluid by Electrophoresis'"),
                        concept2 ->
                            concept2
                                .fhirType(CODEABLECONCEPT)
                                .codingArray(
                                    "coding",
                                    "http://snomed.info/sct|63816008||'Left hepatectomy'")))
        .group("memberOf() function with Coding")
        .testTrue(
            "loinc.memberOf('" + labTestsValueSet + "')",
            "memberOf() returns true for a coding that is in the value set")
        .testFalse(
            "loinc.memberOf('" + proceduresValueSet + "')",
            "memberOf() returns false for a coding that is not in the value set")
        .testFalse(
            "snomed.memberOf('" + labTestsValueSet + "')",
            "memberOf() returns false for a SNOMED coding that is not in the lab tests value set")
        .testTrue(
            "snomed.memberOf('" + proceduresValueSet + "')",
            "memberOf() returns true for a SNOMED coding that is in the procedures value set")
        .testEmpty(
            "emptyCoding.memberOf('" + labTestsValueSet + "')",
            "memberOf() returns empty for an empty coding")
        .group("memberOf() function with collections of Codings")
        .testEquals(
            List.of(true, false, false),
            "multipleCodings.memberOf('" + labTestsValueSet + "')",
            "memberOf() returns correct results for a collection of codings with lab tests value"
                + " set")
        .testEquals(
            List.of(false, true, false),
            "multipleCodings.memberOf('" + proceduresValueSet + "')",
            "memberOf() returns correct results for a collection of codings with procedures value"
                + " set")
        .testEquals(
            List.of(false, false, true),
            "multipleCodings.memberOf('" + vitalSignsValueSet + "')",
            "memberOf() returns correct results for a collection of codings with vital signs value"
                + " set")
        .group("memberOf() function with CodeableConcept")
        .testTrue(
            "loincConcept.memberOf('" + labTestsValueSet + "')",
            "memberOf() returns true for a CodeableConcept with a coding that is in the value set")
        .testFalse(
            "loincConcept.memberOf('" + proceduresValueSet + "')",
            "memberOf() returns false for a CodeableConcept with no codings in the value set")
        .testTrue(
            "snomedConcept.memberOf('" + proceduresValueSet + "')",
            "memberOf() returns true for a CodeableConcept with a coding that is in the value set")
        .testTrue(
            "mixedConcept.memberOf('" + labTestsValueSet + "')",
            "memberOf() returns true for a CodeableConcept with at least one coding in the value"
                + " set")
        .testTrue(
            "mixedConcept.memberOf('" + proceduresValueSet + "')",
            "memberOf() returns true for a CodeableConcept with at least one coding in the value"
                + " set")
        .group("memberOf() function with collections of CodeableConcepts")
        .testEquals(
            List.of(true, false),
            "multipleConcepts.memberOf('" + labTestsValueSet + "')",
            "memberOf() returns correct results for a collection of CodeableConcepts with lab tests"
                + " value set")
        .testEquals(
            List.of(false, true),
            "multipleConcepts.memberOf('" + proceduresValueSet + "')",
            "memberOf() returns correct results for a collection of CodeableConcepts with"
                + " procedures value set")
        .group("memberOf() function error cases")
        .testError(
            "'string'.memberOf('" + labTestsValueSet + "')",
            "memberOf() throws an error when called on a non-coding/non-codeableconcept type")
        .testError("loinc.memberOf()", "memberOf() throws an error when called with no parameters")
        .testError(
            "loinc.memberOf('" + labTestsValueSet + "', 'extra')",
            "memberOf() throws an error when called with more than one parameter")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testTranslateFunction() {
    // Reset mocks and setup terminology service expectations
    SharedMocks.resetAll();

    // Create source codings for testing
    final Coding loincCoding =
        new Coding(
            "http://loinc.org",
            "55915-3",
            "Beta 2 globulin [Mass/volume] in Cerebral spinal fluid by Electrophoresis");
    final Coding snomedCoding =
        new Coding("http://snomed.info/sct", "63816008", "Left hepatectomy");

    // Create target codings for translations
    final Coding translatedLoinc1 =
        new Coding("http://example.org/alt-loinc", "L55915", "Beta 2 globulin");
    final Coding translatedLoinc2 =
        new Coding("http://example.org/alt-loinc", "L55915-ALT", "Beta-2 globulin alt");
    final Coding translatedSnomed =
        new Coding("http://example.org/alt-snomed", "S63816", "Left hepatic resection");

    // Define concept map URLs
    final String conceptMap1 = "http://example.org/fhir/ConceptMap/loinc-to-alt";
    final String conceptMap2 = "http://example.org/fhir/ConceptMap/snomed-to-alt";

    // Setup terminology service expectations for translate
    TerminologyServiceHelpers.setupTranslate(terminologyService)
        // Forward translations (reverse=false)
        .withTranslations(
            loincCoding,
            conceptMap1,
            false,
            TerminologyService.Translation.of(ConceptMapEquivalence.EQUIVALENT, translatedLoinc1),
            TerminologyService.Translation.of(ConceptMapEquivalence.NARROWER, translatedLoinc2))
        .withTranslations(
            snomedCoding,
            conceptMap1,
            false,
            TerminologyService.Translation.of(ConceptMapEquivalence.EQUIVALENT, translatedSnomed))
        .withTranslations(
            snomedCoding,
            conceptMap2,
            false,
            TerminologyService.Translation.of(ConceptMapEquivalence.EQUIVALENT, translatedSnomed))

        // Reverse translations (reverse=true)
        .withTranslations(
            translatedLoinc1,
            conceptMap1,
            true,
            TerminologyService.Translation.of(ConceptMapEquivalence.EQUIVALENT, loincCoding))
        .withTranslations(
            translatedSnomed,
            conceptMap2,
            true,
            TerminologyService.Translation.of(ConceptMapEquivalence.EQUIVALENT, snomedCoding));

    return builder()
        .withSubject(
            sb ->
                sb
                    // Empty coding
                    .coding("emptyCoding", null)
                    // Single codings
                    .coding(
                        "loinc",
                        "http://loinc.org|55915-3||'Beta 2 globulin [Mass/volume] in Cerebral"
                            + " spinal fluid by Electrophoresis'")
                    .coding("snomed", "http://snomed.info/sct|63816008||'Left hepatectomy'")
                    // Array of codings
                    .codingArray(
                        "multipleCodings",
                        "http://loinc.org|55915-3||'Beta 2 globulin [Mass/volume] in Cerebral"
                            + " spinal fluid by Electrophoresis'",
                        "http://snomed.info/sct|63816008||'Left hepatectomy'")
                    // CodeableConcepts with single coding
                    .element(
                        "loincConcept",
                        concept ->
                            concept
                                .fhirType(CODEABLECONCEPT)
                                .codingArray(
                                    "coding",
                                    "http://loinc.org|55915-3||'Beta 2 globulin [Mass/volume] in"
                                        + " Cerebral spinal fluid by Electrophoresis'"))
                    .element(
                        "snomedConcept",
                        concept ->
                            concept
                                .fhirType(CODEABLECONCEPT)
                                .codingArray(
                                    "coding",
                                    "http://snomed.info/sct|63816008||'Left hepatectomy'"))
                    // CodeableConcept with multiple codings
                    .element(
                        "mixedConcept",
                        concept ->
                            concept
                                .fhirType(CODEABLECONCEPT)
                                .codingArray(
                                    "coding",
                                    "http://loinc.org|55915-3||'Beta 2 globulin [Mass/volume] in"
                                        + " Cerebral spinal fluid by Electrophoresis'",
                                    "http://snomed.info/sct|63816008||'Left hepatectomy'"))
                    // Translated codings for reverse testing
                    .coding(
                        "translatedLoinc", "http://example.org/alt-loinc|L55915||'Beta 2 globulin'")
                    .coding(
                        "translatedSnomed",
                        "http://example.org/alt-snomed|S63816||'Left hepatic resection'"))
        .group("translate() function with default parameters")
        .testEquals(
            toCoding("http://example.org/alt-loinc|L55915||'Beta 2 globulin'"),
            "loinc.translate('" + conceptMap1 + "')",
            "translate() returns equivalent translation for a LOINC coding")
        .testEquals(
            toCoding("http://example.org/alt-snomed|S63816||'Left hepatic resection'"),
            "snomed.translate('" + conceptMap2 + "')",
            "translate() returns equivalent translation for a SNOMED coding")
        .testEmpty(
            "emptyCoding.translate('" + conceptMap1 + "')",
            "translate() returns empty for an empty coding")
        .group("translate() function with reverse parameter")
        .testEquals(
            toCoding(
                "http://loinc.org|55915-3||'Beta 2 globulin [Mass/volume] in Cerebral spinal fluid"
                    + " by Electrophoresis'"),
            "translatedLoinc.translate('" + conceptMap1 + "', true)",
            "translate() with reverse=true returns source coding for a translated LOINC coding")
        .testEquals(
            toCoding("http://snomed.info/sct|63816008||'Left hepatectomy'"),
            "translatedSnomed.translate('" + conceptMap2 + "', true)",
            "translate() with reverse=true returns source coding for a translated SNOMED coding")
        .group("translate() function with equivalence parameter")
        .testEquals(
            List.of(
                toCoding("http://example.org/alt-loinc|L55915||'Beta 2 globulin'"),
                toCoding("http://example.org/alt-loinc|L55915-ALT||'Beta-2 globulin alt'")),
            "loinc.translate('" + conceptMap1 + "', false, 'equivalent,narrower')",
            "translate() with equivalence parameter returns translations with specified"
                + " equivalences")
        .testEquals(
            toCoding("http://example.org/alt-loinc|L55915||'Beta 2 globulin'"),
            "loinc.translate('" + conceptMap1 + "', false, 'equivalent')",
            "translate() with equivalence='equivalent' returns only equivalent translations")
        .testEquals(
            toCoding("http://example.org/alt-loinc|L55915-ALT||'Beta-2 globulin alt'"),
            "loinc.translate('" + conceptMap1 + "', false, 'narrower')",
            "translate() with equivalence='narrower' returns only narrower translations")
        .group("translate() function on collections")
        .testEquals(
            List.of(
                toCoding("http://example.org/alt-loinc|L55915||'Beta 2 globulin'"),
                toCoding("http://example.org/alt-snomed|S63816||'Left hepatic resection'")),
            "multipleCodings.translate('" + conceptMap1 + "')",
            "translate() returns translations for a collection of codings")
        .group("translate() function with CodeableConcept")
        .testEquals(
            toCoding("http://example.org/alt-loinc|L55915||'Beta 2 globulin'"),
            "loincConcept.translate('" + conceptMap1 + "')",
            "translate() returns translation for a CodeableConcept with LOINC coding")
        .testEquals(
            toCoding("http://example.org/alt-snomed|S63816||'Left hepatic resection'"),
            "snomedConcept.translate('" + conceptMap2 + "')",
            "translate() returns translation for a CodeableConcept with SNOMED coding")
        .testEquals(
            List.of(
                toCoding("http://example.org/alt-loinc|L55915||'Beta 2 globulin'"),
                toCoding("http://example.org/alt-snomed|S63816||'Left hepatic resection'")),
            "mixedConcept.translate('" + conceptMap1 + "')",
            "translate() returns translations for a CodeableConcept with multiple codings")
        .group("translate() function error cases")
        .testError(
            "'string'.translate('" + conceptMap1 + "')",
            "translate() throws an error when called on a non-coding/non-codeableconcept type")
        .testError(
            "loinc.translate()", "translate() throws an error when called with no parameters")
        .testError(
            "loinc.translate('" + conceptMap1 + "', false, 'equivalent', 'target', 'extra')",
            "translate() throws an error when called with more than four parameters")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testSubsumesFunctions() {
    // Reset mocks and setup terminology service expectations
    SharedMocks.resetAll();

    // Create codings for testing
    final Coding liverResection =
        new Coding("http://snomed.info/sct", "107963000", "Liver resection");
    final Coding leftHepatectomy =
        new Coding("http://snomed.info/sct", "63816008", "Left hepatectomy");
    final Coding viralSinusitis =
        new Coding("http://snomed.info/sct", "444814009", "Viral sinusitis");
    final Coding chronicSinusitis =
        new Coding("http://snomed.info/sct", "40055000", "Chronic sinusitis");

    // Setup terminology service expectations for subsumes
    TerminologyServiceHelpers.setupSubsumes(terminologyService)
        // Liver resection subsumes Left hepatectomy
        .withSubsumes(liverResection, leftHepatectomy)
        // Viral sinusitis subsumes Chronic sinusitis
        .withSubsumes(viralSinusitis, chronicSinusitis);

    return builder()
        .withSubject(
            sb ->
                sb
                    // Empty coding
                    .codingEmpty("emptyCoding")
                    // Single codings
                    .coding("liverResection", "http://snomed.info/sct|107963000||'Liver resection'")
                    .coding(
                        "leftHepatectomy", "http://snomed.info/sct|63816008||'Left hepatectomy'")
                    .coding("viralSinusitis", "http://snomed.info/sct|444814009||'Viral sinusitis'")
                    .coding(
                        "chronicSinusitis", "http://snomed.info/sct|40055000||'Chronic sinusitis'")
                    // Arrays of codings
                    .codingArray(
                        "procedureCodings",
                        "http://snomed.info/sct|107963000||'Liver resection'",
                        "http://snomed.info/sct|63816008||'Left hepatectomy'")
                    .codingArray(
                        "sinusitisCodings",
                        "http://snomed.info/sct|444814009||'Viral sinusitis'",
                        "http://snomed.info/sct|40055000||'Chronic sinusitis'")
                    // CodeableConcepts
                    .element(
                        "liverResectionConcept",
                        concept ->
                            concept
                                .fhirType(CODEABLECONCEPT)
                                .codingArray(
                                    "coding",
                                    "http://snomed.info/sct|107963000||'Liver resection'"))
                    .element(
                        "leftHepatectomyConcept",
                        concept ->
                            concept
                                .fhirType(CODEABLECONCEPT)
                                .codingArray(
                                    "coding",
                                    "http://snomed.info/sct|63816008||'Left hepatectomy'")))
        .group("subsumes() function with Coding")
        .testTrue(
            "liverResection.subsumes(leftHepatectomy)",
            "subsumes() returns true when the source coding subsumes the target coding")
        .testFalse(
            "leftHepatectomy.subsumes(liverResection)",
            "subsumes() returns false when the source coding does not subsume the target coding")
        .testTrue(
            "viralSinusitis.subsumes(chronicSinusitis)",
            "subsumes() returns true when the source coding subsumes the target coding (different"
                + " concepts)")
        .testFalse(
            "chronicSinusitis.subsumes(viralSinusitis)",
            "subsumes() returns false when the source coding does not subsume the target coding"
                + " (different concepts)")
        .testTrue(
            "liverResection.subsumes(liverResection)",
            "subsumes() returns true when the source coding is equivalent to the target coding")
        .testEmpty(
            "emptyCoding.subsumes(leftHepatectomy)",
            "subsumes() returns empty when the source coding is empty")
        .testEmpty(
            "liverResection.subsumes(emptyCoding)",
            "subsumes() returns empty when the target coding is empty")
        .group("subsumes() function with collections")
        .testEquals(
            List.of(true, false),
            "procedureCodings.subsumes(liverResection)",
            "subsumes() returns correct results for a collection of source codings")
        .testEquals(
            true,
            "liverResection.subsumes(procedureCodings)",
            "subsumes() returns correct results for a collection of target codings")
        .testEquals(
            List.of(true, true),
            "procedureCodings.subsumes(procedureCodings)",
            "subsumes() returns correct results for collections of both source and target codings")
        .group("subsumes() function with CodeableConcept")
        .testTrue(
            "liverResectionConcept.subsumes(leftHepatectomyConcept)",
            "subsumes() returns true when the source CodeableConcept subsumes the target"
                + " CodeableConcept")
        .testFalse(
            "leftHepatectomyConcept.subsumes(liverResectionConcept)",
            "subsumes() returns false when the source CodeableConcept does not subsume the target"
                + " CodeableConcept")
        .group("subsumedBy() function with Coding")
        .testTrue(
            "leftHepatectomy.subsumedBy(liverResection)",
            "subsumedBy() returns true when the source coding is subsumed by the target coding")
        .testFalse(
            "liverResection.subsumedBy(leftHepatectomy)",
            "subsumedBy() returns false when the source coding is not subsumed by the target"
                + " coding")
        .testTrue(
            "chronicSinusitis.subsumedBy(viralSinusitis)",
            "subsumedBy() returns true when the source coding is subsumed by the target coding"
                + " (different concepts)")
        .testFalse(
            "viralSinusitis.subsumedBy(chronicSinusitis)",
            "subsumedBy() returns false when the source coding is not subsumed by the target coding"
                + " (different concepts)")
        .testTrue(
            "liverResection.subsumedBy(liverResection)",
            "subsumedBy() returns true when the source coding is equivalent to the target coding")
        .testEmpty(
            "emptyCoding.subsumedBy(liverResection)",
            "subsumedBy() returns empty when the source coding is empty")
        .testEmpty(
            "leftHepatectomy.subsumedBy(emptyCoding)",
            "subsumedBy() returns empty when the target coding is empty")
        .group("subsumedBy() function with collections")
        .testEquals(
            List.of(false, true),
            "procedureCodings.subsumedBy(leftHepatectomy)",
            "subsumedBy() returns correct results for a collection of source codings")
        .testEquals(
            true,
            "leftHepatectomy.subsumedBy(procedureCodings)",
            "subsumedBy() returns correct results for a collection of target codings")
        .group("subsumedBy() function with CodeableConcept")
        .testTrue(
            "leftHepatectomyConcept.subsumedBy(liverResectionConcept)",
            "subsumedBy() returns true when the source CodeableConcept is subsumed by the target"
                + " CodeableConcept")
        .testFalse(
            "liverResectionConcept.subsumedBy(leftHepatectomyConcept)",
            "subsumedBy() returns false when the source CodeableConcept is not subsumed by the"
                + " target CodeableConcept")
        .group("subsumes/subsumedBy error cases")
        .testError(
            "'string'.subsumes(leftHepatectomy)",
            "subsumes() throws an error when called on a non-coding/non-codeableconcept type")
        .testError(
            "liverResection.subsumes('string')",
            "subsumes() throws an error when called with a non-coding/non-codeableconcept argument")
        .testError(
            "'string'.subsumedBy(liverResection)",
            "subsumedBy() throws an error when called on a non-coding/non-codeableconcept type")
        .testError(
            "leftHepatectomy.subsumedBy('string')",
            "subsumedBy() throws an error when called with a non-coding/non-codeableconcept"
                + " argument")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testDesignationFunction() {
    // Reset mocks and setup terminology service expectations
    SharedMocks.resetAll();

    // Create codings for testing
    final Coding loincCoding =
        new Coding(
            "http://loinc.org",
            "55915-3",
            "Beta 2 globulin [Mass/volume] in Cerebral spinal fluid by Electrophoresis");
    final Coding snomedCoding =
        new Coding("http://snomed.info/sct", "63816008", "Left hepatectomy");

    // Create use coding for filtering designations
    final Coding synonymUse = new Coding("http://snomed.info/sct", "900000000000013009", "Synonym");

    // Setup terminology service expectations for designations
    TerminologyServiceHelpers.setupLookup(terminologyService)
        // Setup designations with no filtering
        .withDesignation(loincCoding, null, null, "Beta-2 globulin", "β2 globulin")
        .withDesignation(snomedCoding, null, null, "Left hepatic lobectomy", "Left liver resection")

        // Setup designations with use filtering
        .withDesignation(loincCoding, synonymUse, null, "Beta-2 globulin")
        .withDesignation(snomedCoding, synonymUse, null, "Left hepatic lobectomy")

        // Setup designations with language filtering
        .withDesignation(loincCoding, null, "fr", "Globuline bêta-2")
        .withDesignation(snomedCoding, null, "fr", "Hépatectomie gauche")

        // Setup designations with both use and language filtering
        .withDesignation(loincCoding, synonymUse, "de", "Beta-2-Globulin")
        .withDesignation(snomedCoding, synonymUse, "de", "Linke Hepatektomie")

        // Additional designations for language filtering
        .withDesignation(loincCoding, null, "de", "Beta-2-Globulin (allgemein)")
        .withDesignation(snomedCoding, null, "de", "Linke Hepatektomie (allgemein)")
        .done();

    return builder()
        .withSubject(
            sb ->
                sb
                    // Empty coding
                    .coding("emptyCoding", null)
                    // Single codings
                    .coding(
                        "loinc",
                        "http://loinc.org|55915-3||'Beta 2 globulin [Mass/volume] in Cerebral"
                            + " spinal fluid by Electrophoresis'")
                    .coding("snomed", "http://snomed.info/sct|63816008||'Left hepatectomy'")
                    // Array of codings
                    .codingArray(
                        "multipleCodings",
                        "http://loinc.org|55915-3||'Beta 2 globulin [Mass/volume] in Cerebral"
                            + " spinal fluid by Electrophoresis'",
                        "http://snomed.info/sct|63816008||'Left hepatectomy'"))
        .group("designation() function with no parameters")
        .testEquals(
            List.of(
                "Beta-2 globulin",
                "β2 globulin",
                "Beta-2 globulin",
                "Globuline bêta-2",
                "Beta-2-Globulin",
                "Beta-2-Globulin (allgemein)"),
            "loinc.designation()",
            "designation() returns all designations for a single LOINC coding")
        .testEquals(
            List.of(
                "Left hepatic lobectomy",
                "Left liver resection",
                "Left hepatic lobectomy",
                "Hépatectomie gauche",
                "Linke Hepatektomie",
                "Linke Hepatektomie (allgemein)"),
            "snomed.designation()",
            "designation() returns all designations for a single SNOMED coding")
        .testEmpty("emptyCoding.designation()", "designation() returns empty for an empty coding")
        .group("designation() function with use parameter")
        .testEquals(
            List.of("Beta-2 globulin", "Beta-2-Globulin"),
            "loinc.designation(http://snomed.info/sct|900000000000013009)",
            "designation() with use parameter returns filtered designations for LOINC")
        .testEquals(
            List.of("Left hepatic lobectomy", "Linke Hepatektomie"),
            "snomed.designation(http://snomed.info/sct|900000000000013009)",
            "designation() with use parameter returns filtered designations for SNOMED")
        .testEmpty(
            "emptyCoding.designation(http://snomed.info/sct|900000000000013009)",
            "designation() with use parameter returns empty for an empty coding")
        .group("designation() function with both use and language parameters")
        .testEquals(
            List.of("Beta-2-Globulin"),
            "loinc.designation(http://snomed.info/sct|900000000000013009, 'de')",
            "designation() with use and language parameters returns filtered designations for"
                + " LOINC")
        .testEquals(
            List.of("Linke Hepatektomie"),
            "snomed.designation(http://snomed.info/sct|900000000000013009, 'de')",
            "designation() with use and language parameters returns filtered designations for"
                + " SNOMED")
        .testEmpty(
            "emptyCoding.designation(http://snomed.info/sct|900000000000013009, 'de')",
            "designation() with use and language parameters returns empty for an empty coding")
        .group("designation() function on collections")
        .testEquals(
            List.of(
                "Beta-2 globulin",
                "β2 globulin",
                "Beta-2 globulin",
                "Globuline bêta-2",
                "Beta-2-Globulin",
                "Beta-2-Globulin (allgemein)",
                "Left hepatic lobectomy",
                "Left liver resection",
                "Left hepatic lobectomy",
                "Hépatectomie gauche",
                "Linke Hepatektomie",
                "Linke Hepatektomie (allgemein)"),
            "multipleCodings.designation()",
            "designation() returns all designations on a collection of codings")
        .testEquals(
            List.of(
                "Beta-2 globulin",
                "Beta-2-Globulin",
                "Left hepatic lobectomy",
                "Linke Hepatektomie"),
            "multipleCodings.designation(http://snomed.info/sct|900000000000013009)",
            "designation() with use parameter returns filtered designations on a collection of"
                + " codings")
        .testEquals(
            List.of("Beta-2-Globulin", "Linke Hepatektomie"),
            "multipleCodings.designation(http://snomed.info/sct|900000000000013009, 'de')",
            "designation() with use and language parameters returns filtered designations on a"
                + " collection of codings")
        .group("designation() function error cases")
        .testError(
            "'string'.designation()",
            "designation() throws an error when called on a non-coding type")
        .testError(
            "loinc.designation(http://snomed.info/sct|900000000000013009, 'de', 'extra')",
            "designation() throws an error when called with more than two parameters")
        .build();
  }
}
