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

package au.csiro.pathling.search;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.datasource.ObjectDataSource;
import java.math.BigDecimal;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Address.AddressUse;
import org.hl7.fhir.r4.model.AuditEvent;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Condition;
import org.hl7.fhir.r4.model.ContactPoint.ContactPointSystem;
import org.hl7.fhir.r4.model.Coverage;
import org.hl7.fhir.r4.model.DateTimeType;
import org.hl7.fhir.r4.model.DateType;
import org.hl7.fhir.r4.model.DecimalType;
import org.hl7.fhir.r4.model.Enumerations.AdministrativeGender;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.Identifier;
import org.hl7.fhir.r4.model.InstantType;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Period;
import org.hl7.fhir.r4.model.Quantity;
import org.hl7.fhir.r4.model.RiskAssessment;
import org.hl7.fhir.r4.model.RiskAssessment.RiskAssessmentPredictionComponent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;

/** Tests for {@link FhirSearchExecutor}. */
@SpringBootUnitTest
@TestInstance(Lifecycle.PER_CLASS)
@Slf4j
class FhirSearchExecutorTest {

  @Autowired SparkSession spark;

  @Autowired FhirEncoders encoders;

  private final SearchParameterRegistry registry = new TestSearchParameterRegistry();

  // ========== Parameterized search tests ==========

  Stream<Arguments> searchTestCases() {
    return Stream.of(
        // Gender tests
        Arguments.of(
            "gender search male",
            ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSource,
            "gender",
            List.of("male"),
            Set.of("1", "3")),
        Arguments.of(
            "gender search female",
            ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSource,
            "gender",
            List.of("female"),
            Set.of("2")),
        Arguments.of(
            "gender search multiple values",
            ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSource,
            "gender",
            List.of("male", "female"),
            Set.of("1", "2", "3")),
        Arguments.of(
            "gender search no matches",
            ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSource,
            "gender",
            List.of("other"),
            Set.of()),

        // Address-use tests
        Arguments.of(
            "address-use search home",
            ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithAddresses,
            "address-use",
            List.of("home"),
            Set.of("1", "2")),
        Arguments.of(
            "address-use search work",
            ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithAddresses,
            "address-use",
            List.of("work"),
            Set.of("2")),
        Arguments.of(
            "address-use search no matches",
            ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithAddresses,
            "address-use",
            List.of("billing"),
            Set.of()),
        Arguments.of(
            "address-use search multiple values",
            ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithAddresses,
            "address-use",
            List.of("home", "temp"),
            Set.of("1", "2", "3")),

        // Family tests
        Arguments.of(
            "family search exact match",
            ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithNames,
            "family",
            List.of("Smith"),
            Set.of("1")),
        Arguments.of(
            "family search case insensitive",
            ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithNames,
            "family",
            List.of("smith"),
            Set.of("1")),
        Arguments.of(
            "family search starts with",
            ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithNames,
            "family",
            List.of("Smi"),
            Set.of("1")),
        Arguments.of(
            "family search no match",
            ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithNames,
            "family",
            List.of("Williams"),
            Set.of()),
        Arguments.of(
            "family search multiple names",
            ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithNames,
            "family",
            List.of("John"),
            Set.of("2")),
        Arguments.of(
            "family search multiple values",
            ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithNames,
            "family",
            List.of("Smith", "Jones"),
            Set.of("1", "2")),

        // :not modifier tests (token type)
        Arguments.of(
            "gender:not search excludes male",
            ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSource,
            "gender:not",
            List.of("male"),
            Set.of("2", "4")), // female + no gender
        Arguments.of(
            "gender:not search excludes female",
            ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSource,
            "gender:not",
            List.of("female"),
            Set.of("1", "3", "4")), // male + no gender

        // :exact modifier tests (string type)
        Arguments.of(
            "family:exact search exact match",
            ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithNames,
            "family:exact",
            List.of("Smith"),
            Set.of("1")),
        Arguments.of(
            "family:exact search wrong case",
            ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithNames,
            "family:exact",
            List.of("smith"),
            Set.of()), // case-sensitive - no match
        Arguments.of(
            "family:exact search partial",
            ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithNames,
            "family:exact",
            List.of("Smi"),
            Set.of()), // prefix doesn't match

        // Birthdate tests (date type) - basic scenarios only, precision tests are in
        // ElementMatcherTest
        Arguments.of(
            "birthdate search match",
            ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithBirthDates,
            "birthdate",
            List.of("1990-01-15"),
            Set.of("1", "3")),
        Arguments.of(
            "birthdate search no match",
            ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithBirthDates,
            "birthdate",
            List.of("2000-01-01"),
            Set.of()),
        Arguments.of(
            "birthdate search with datetime value",
            ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithBirthDates,
            "birthdate",
            List.of("1990-01-15T10:00"),
            Set.of("1", "3")),
        Arguments.of(
            "birthdate search multiple values",
            ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithBirthDates,
            "birthdate",
            List.of("1990-01-15", "1985-06-20"),
            Set.of("1", "2", "3")),

        // Date prefix tests - basic scenarios only, exhaustive tests are in ElementMatcherTest
        // ge - greater or equal
        Arguments.of(
            "birthdate ge prefix",
            ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithBirthDates,
            "birthdate",
            List.of("ge1990-01-15"),
            Set.of("1", "3")), // born on or after Jan 15, 1990
        Arguments.of(
            "birthdate ge prefix with year",
            ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithBirthDates,
            "birthdate",
            List.of("ge1986"),
            Set.of("1", "3")), // born 1986 or later

        // le - less or equal
        Arguments.of(
            "birthdate le prefix",
            ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithBirthDates,
            "birthdate",
            List.of("le1989"),
            Set.of("2")), // born 1989 or earlier

        // gt - greater than
        Arguments.of(
            "birthdate gt prefix",
            ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithBirthDates,
            "birthdate",
            List.of("gt1985-06-20"),
            Set.of("1", "3")), // born after Jun 20, 1985

        // lt - less than
        Arguments.of(
            "birthdate lt prefix",
            ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithBirthDates,
            "birthdate",
            List.of("lt1990-01-15"),
            Set.of("2")), // born before Jan 15, 1990

        // ne - not equal
        Arguments.of(
            "birthdate ne prefix",
            ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithBirthDates,
            "birthdate",
            List.of("ne1990-01-15"),
            Set.of("2")), // NOT born on Jan 15, 1990

        // ========== RiskAssessment probability tests (number type) ==========
        // eq - equals (default)
        Arguments.of(
            "probability exact match",
            ResourceType.RISKASSESSMENT,
            (Supplier<ObjectDataSource>) this::createRiskAssessmentDataSource,
            "probability",
            List.of("0.5"),
            Set.of("2")),
        Arguments.of(
            "probability eq prefix",
            ResourceType.RISKASSESSMENT,
            (Supplier<ObjectDataSource>) this::createRiskAssessmentDataSource,
            "probability",
            List.of("eq0.5"),
            Set.of("2")),
        Arguments.of(
            "probability multiple values",
            ResourceType.RISKASSESSMENT,
            (Supplier<ObjectDataSource>) this::createRiskAssessmentDataSource,
            "probability",
            List.of("0.2", "0.8"),
            Set.of("1", "3")),

        // gt - greater than
        Arguments.of(
            "probability gt prefix",
            ResourceType.RISKASSESSMENT,
            (Supplier<ObjectDataSource>) this::createRiskAssessmentDataSource,
            "probability",
            List.of("gt0.5"),
            Set.of("3")),

        // ge - greater or equal
        Arguments.of(
            "probability ge prefix",
            ResourceType.RISKASSESSMENT,
            (Supplier<ObjectDataSource>) this::createRiskAssessmentDataSource,
            "probability",
            List.of("ge0.5"),
            Set.of("2", "3")),

        // lt - less than
        Arguments.of(
            "probability lt prefix",
            ResourceType.RISKASSESSMENT,
            (Supplier<ObjectDataSource>) this::createRiskAssessmentDataSource,
            "probability",
            List.of("lt0.5"),
            Set.of("1")),

        // le - less or equal
        Arguments.of(
            "probability le prefix",
            ResourceType.RISKASSESSMENT,
            (Supplier<ObjectDataSource>) this::createRiskAssessmentDataSource,
            "probability",
            List.of("le0.5"),
            Set.of("1", "2")),

        // ne - not equal
        Arguments.of(
            "probability ne prefix",
            ResourceType.RISKASSESSMENT,
            (Supplier<ObjectDataSource>) this::createRiskAssessmentDataSource,
            "probability",
            List.of("ne0.5"),
            Set.of("1", "3")),

        // ========== Coverage period tests (Period type date search) ==========
        // eq - ranges overlap
        Arguments.of(
            "period search within range",
            ResourceType.COVERAGE,
            (Supplier<ObjectDataSource>) this::createCoverageDataSource,
            "period",
            List.of("2023-03-15"),
            Set.of("1")), // within Coverage 1's period
        Arguments.of(
            "period search overlapping periods",
            ResourceType.COVERAGE,
            (Supplier<ObjectDataSource>) this::createCoverageDataSource,
            "period",
            List.of("2023-06-15"),
            Set.of("1", "2")), // overlaps both
        Arguments.of(
            "period search month precision",
            ResourceType.COVERAGE,
            (Supplier<ObjectDataSource>) this::createCoverageDataSource,
            "period",
            List.of("2023-06"),
            Set.of("1", "2")), // June overlaps both

        // ge - starts at or after parameter start
        Arguments.of(
            "period ge prefix",
            ResourceType.COVERAGE,
            (Supplier<ObjectDataSource>) this::createCoverageDataSource,
            "period",
            List.of("ge2023-06-01"),
            Set.of("2", "3")), // Coverage 2 and 3 start >= June 1

        // le - ends at or before parameter end
        Arguments.of(
            "period le prefix",
            ResourceType.COVERAGE,
            (Supplier<ObjectDataSource>) this::createCoverageDataSource,
            "period",
            List.of("le2023-06-30"),
            Set.of("1", "4")), // Coverage 1 and 4 end <= June 30

        // gt - ends after parameter
        Arguments.of(
            "period gt prefix",
            ResourceType.COVERAGE,
            (Supplier<ObjectDataSource>) this::createCoverageDataSource,
            "period",
            List.of("gt2023-12-31"),
            Set.of("3")), // only open-ended Coverage 3

        // lt - starts before parameter
        Arguments.of(
            "period lt prefix",
            ResourceType.COVERAGE,
            (Supplier<ObjectDataSource>) this::createCoverageDataSource,
            "period",
            List.of("lt2023-01-01"),
            Set.of("4")), // only Coverage 4 (null start = -infinity)

        // ne - no overlap
        Arguments.of(
            "period ne prefix",
            ResourceType.COVERAGE,
            (Supplier<ObjectDataSource>) this::createCoverageDataSource,
            "period",
            List.of("ne2023-03-15"),
            Set.of("2", "3", "4")), // doesn't overlap with Mar 15

        // ========== Condition recorded-date tests (dateTime type) ==========
        // eq - day precision matching dateTime
        Arguments.of(
            "recorded-date search day precision",
            ResourceType.CONDITION,
            (Supplier<ObjectDataSource>) this::createConditionDataSource,
            "recorded-date",
            List.of("2023-03-15"),
            Set.of("1", "3")), // matches Condition 1 and 3
        Arguments.of(
            "recorded-date search no match",
            ResourceType.CONDITION,
            (Supplier<ObjectDataSource>) this::createConditionDataSource,
            "recorded-date",
            List.of("2023-01-01"),
            Set.of()), // no match

        // ge - greater or equal
        Arguments.of(
            "recorded-date ge prefix",
            ResourceType.CONDITION,
            (Supplier<ObjectDataSource>) this::createConditionDataSource,
            "recorded-date",
            List.of("ge2023-06-01"),
            Set.of("2")), // Condition 2 only

        // lt - less than
        Arguments.of(
            "recorded-date lt prefix",
            ResourceType.CONDITION,
            (Supplier<ObjectDataSource>) this::createConditionDataSource,
            "recorded-date",
            List.of("lt2023-06-01"),
            Set.of("1", "3")), // Condition 1 and 3

        // ========== AuditEvent date tests (instant type) ==========
        // eq - instant matching with day precision
        Arguments.of(
            "date search day precision",
            ResourceType.AUDITEVENT,
            (Supplier<ObjectDataSource>) this::createAuditEventDataSource,
            "date",
            List.of("2023-03-15"),
            Set.of("1", "3")), // matches AuditEvent 1 and 3
        Arguments.of(
            "date search no match",
            ResourceType.AUDITEVENT,
            (Supplier<ObjectDataSource>) this::createAuditEventDataSource,
            "date",
            List.of("2023-01-01"),
            Set.of()), // no match

        // ge - greater or equal
        Arguments.of(
            "date ge prefix",
            ResourceType.AUDITEVENT,
            (Supplier<ObjectDataSource>) this::createAuditEventDataSource,
            "date",
            List.of("ge2023-06-01"),
            Set.of("2")), // AuditEvent 2 only

        // ne - not equal
        Arguments.of(
            "date ne prefix",
            ResourceType.AUDITEVENT,
            (Supplier<ObjectDataSource>) this::createAuditEventDataSource,
            "date",
            List.of("ne2023-03-15"),
            Set.of("2")), // only AuditEvent 2

        // ========== Patient identifier tests (Identifier type - system|value) ==========
        Arguments.of(
            "identifier search code only",
            ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithIdentifiers,
            "identifier",
            List.of("12345"),
            Set.of("1")),
        Arguments.of(
            "identifier search system|code",
            ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithIdentifiers,
            "identifier",
            List.of("http://hospital.org/mrn|12345"),
            Set.of("1")),
        Arguments.of(
            "identifier search system|code wrong system",
            ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithIdentifiers,
            "identifier",
            List.of("http://other.org/mrn|12345"),
            Set.of()), // wrong system
        Arguments.of(
            "identifier search |code (no system)",
            ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithIdentifiers,
            "identifier",
            List.of("|99999"),
            Set.of("3")), // identifier with null system
        Arguments.of(
            "identifier search system| (any code)",
            ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithIdentifiers,
            "identifier",
            List.of("http://hospital.org/mrn|"),
            Set.of("1", "2")), // any code in system

        // ========== Patient telecom tests (ContactPoint type - value only) ==========
        Arguments.of(
            "telecom search phone",
            ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithTelecoms,
            "telecom",
            List.of("555-1234"),
            Set.of("1")),
        Arguments.of(
            "telecom search email",
            ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithTelecoms,
            "telecom",
            List.of("john@example.com"),
            Set.of("1")),
        Arguments.of(
            "telecom search no match",
            ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithTelecoms,
            "telecom",
            List.of("999-9999"),
            Set.of()),
        Arguments.of(
            "telecom search multiple values",
            ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithTelecoms,
            "telecom",
            List.of("555-1234", "555-5678"),
            Set.of("1", "2")),

        // ========== Patient active tests (boolean type) ==========
        Arguments.of(
            "active search true",
            ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithActive,
            "active",
            List.of("true"),
            Set.of("1", "3")),
        Arguments.of(
            "active search false",
            ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithActive,
            "active",
            List.of("false"),
            Set.of("2")),

        // ========== Observation code tests (CodeableConcept type - system|code) ==========
        Arguments.of(
            "code search code only",
            ResourceType.OBSERVATION,
            (Supplier<ObjectDataSource>) this::createObservationDataSource,
            "code",
            List.of("29463-7"),
            Set.of("1", "2")), // body weight code
        Arguments.of(
            "code search system|code",
            ResourceType.OBSERVATION,
            (Supplier<ObjectDataSource>) this::createObservationDataSource,
            "code",
            List.of("http://loinc.org|29463-7"),
            Set.of("1", "2")),
        Arguments.of(
            "code search system|code wrong system",
            ResourceType.OBSERVATION,
            (Supplier<ObjectDataSource>) this::createObservationDataSource,
            "code",
            List.of("http://snomed.info/sct|29463-7"),
            Set.of()), // wrong system
        Arguments.of(
            "code search |code (no system)",
            ResourceType.OBSERVATION,
            (Supplier<ObjectDataSource>) this::createObservationDataSource,
            "code",
            List.of("|local-99"),
            Set.of("3")), // coding with null system
        Arguments.of(
            "code search system| (any code)",
            ResourceType.OBSERVATION,
            (Supplier<ObjectDataSource>) this::createObservationDataSource,
            "code",
            List.of("http://loinc.org|"),
            Set.of("1", "2")), // any LOINC code
        Arguments.of(
            "code search multiple codings match",
            ResourceType.OBSERVATION,
            (Supplier<ObjectDataSource>) this::createObservationDataSource,
            "code",
            List.of("http://snomed.info/sct|27113001"),
            Set.of("2")), // obs 2 has SNOMED coding too

        // ========== Observation date tests (polymorphic effective[x] with multiple types)
        // ==========
        // Observation.effective can be: dateTime, Period, Timing (unsupported), or instant
        // This tests the multi-expression search parameter support

        // eq - matches across dateTime, Period, and instant types
        Arguments.of(
            "observation date search matching dateTime",
            ResourceType.OBSERVATION,
            (Supplier<ObjectDataSource>) this::createObservationDataSourceWithDates,
            "date",
            List.of("2023-03-15"),
            Set.of("1")), // dateTime obs
        Arguments.of(
            "observation date search matching Period",
            ResourceType.OBSERVATION,
            (Supplier<ObjectDataSource>) this::createObservationDataSourceWithDates,
            "date",
            List.of("2023-06-15"),
            Set.of("2")), // within Period obs
        Arguments.of(
            "observation date search matching instant",
            ResourceType.OBSERVATION,
            (Supplier<ObjectDataSource>) this::createObservationDataSourceWithDates,
            "date",
            List.of("2023-09-15"),
            Set.of("3")), // instant obs
        Arguments.of(
            "observation date search matching multiple types",
            ResourceType.OBSERVATION,
            (Supplier<ObjectDataSource>) this::createObservationDataSourceWithDates,
            "date",
            List.of("2023-03-15", "2023-06-15", "2023-09-15"),
            Set.of("1", "2", "3")),

        // ge - greater or equal across types
        Arguments.of(
            "observation date ge matching all",
            ResourceType.OBSERVATION,
            (Supplier<ObjectDataSource>) this::createObservationDataSourceWithDates,
            "date",
            List.of("ge2023-01-01"),
            Set.of("1", "2", "3")), // all on or after Jan 1
        Arguments.of(
            "observation date ge matching some",
            ResourceType.OBSERVATION,
            (Supplier<ObjectDataSource>) this::createObservationDataSourceWithDates,
            "date",
            List.of("ge2023-06-01"),
            Set.of("2", "3")), // Period starts June 1, instant is Sept

        // lt - less than across types
        Arguments.of(
            "observation date lt matching dateTime only",
            ResourceType.OBSERVATION,
            (Supplier<ObjectDataSource>) this::createObservationDataSourceWithDates,
            "date",
            List.of("lt2023-06-01"),
            Set.of("1")), // only dateTime is before June 1

        // Period overlap test
        Arguments.of(
            "observation date Period overlap",
            ResourceType.OBSERVATION,
            (Supplier<ObjectDataSource>) this::createObservationDataSourceWithDates,
            "date",
            List.of("2023-07-01"),
            Set.of("2")), // within Period [June 1 - Aug 31]

        // ========== Observation value-quantity tests (Quantity type) ==========
        // eq (default) - uses range semantics based on significant figures
        // "70" (2 sig figs) matches range [69.5, 70.5)
        Arguments.of(
            "value-quantity eq match",
            ResourceType.OBSERVATION,
            (Supplier<ObjectDataSource>) this::createObservationDataSourceWithQuantities,
            "value-quantity",
            List.of("70"),
            Set.of("1")), // value=70
        Arguments.of(
            "value-quantity eq no match",
            ResourceType.OBSERVATION,
            (Supplier<ObjectDataSource>) this::createObservationDataSourceWithQuantities,
            "value-quantity",
            List.of("71"),
            Set.of()), // no value in [70.5, 71.5)
        Arguments.of(
            "value-quantity eq with precision",
            ResourceType.OBSERVATION,
            (Supplier<ObjectDataSource>) this::createObservationDataSourceWithQuantities,
            "value-quantity",
            List.of("85.5"),
            Set.of("2")), // value=85.5
        Arguments.of(
            "value-quantity multiple values",
            ResourceType.OBSERVATION,
            (Supplier<ObjectDataSource>) this::createObservationDataSourceWithQuantities,
            "value-quantity",
            List.of("70", "120"),
            Set.of("1", "3")),

        // gt - greater than (exact semantics)
        Arguments.of(
            "value-quantity gt prefix",
            ResourceType.OBSERVATION,
            (Supplier<ObjectDataSource>) this::createObservationDataSourceWithQuantities,
            "value-quantity",
            List.of("gt85"),
            Set.of("2", "3")), // 85.5 and 120 > 85

        // ge - greater or equal (exact semantics)
        Arguments.of(
            "value-quantity ge prefix",
            ResourceType.OBSERVATION,
            (Supplier<ObjectDataSource>) this::createObservationDataSourceWithQuantities,
            "value-quantity",
            List.of("ge85.5"),
            Set.of("2", "3")), // 85.5 and 120 >= 85.5

        // lt - less than (exact semantics)
        Arguments.of(
            "value-quantity lt prefix",
            ResourceType.OBSERVATION,
            (Supplier<ObjectDataSource>) this::createObservationDataSourceWithQuantities,
            "value-quantity",
            List.of("lt85"),
            Set.of("1")), // only 70 < 85

        // le - less or equal (exact semantics)
        Arguments.of(
            "value-quantity le prefix",
            ResourceType.OBSERVATION,
            (Supplier<ObjectDataSource>) this::createObservationDataSourceWithQuantities,
            "value-quantity",
            List.of("le85.5"),
            Set.of("1", "2")), // 70 and 85.5 <= 85.5

        // ne - not equal (range semantics)
        Arguments.of(
            "value-quantity ne prefix",
            ResourceType.OBSERVATION,
            (Supplier<ObjectDataSource>) this::createObservationDataSourceWithQuantities,
            "value-quantity",
            List.of("ne70"),
            Set.of("2", "3")), // exclude value in [69.5, 70.5)

        // ========== Observation value-quantity tests with system|code ==========
        // See SPEC_CLARIFICATIONS.md for format rules

        // value|system|code - full match
        Arguments.of(
            "value-quantity with system|code match",
            ResourceType.OBSERVATION,
            (Supplier<ObjectDataSource>) this::createObservationDataSourceWithQuantities,
            "value-quantity",
            List.of("70|http://unitsofmeasure.org|kg"),
            Set.of("1")),
        Arguments.of(
            "value-quantity with system|code wrong system",
            ResourceType.OBSERVATION,
            (Supplier<ObjectDataSource>) this::createObservationDataSourceWithQuantities,
            "value-quantity",
            List.of("70|http://other.org|kg"),
            Set.of()),
        Arguments.of(
            "value-quantity with system|code wrong code",
            ResourceType.OBSERVATION,
            (Supplier<ObjectDataSource>) this::createObservationDataSourceWithQuantities,
            "value-quantity",
            List.of("70|http://unitsofmeasure.org|g"),
            Set.of()),

        // value||code - any system, exact code
        // Empty system means "any system" (no constraint on system)
        Arguments.of(
            "value-quantity with ||code matches any system",
            ResourceType.OBSERVATION,
            (Supplier<ObjectDataSource>) this::createObservationDataSourceWithQuantities,
            "value-quantity",
            List.of("70||kg"),
            Set.of("1")), // matches UCUM kg
        Arguments.of(
            "value-quantity with ||code matches null system too",
            ResourceType.OBSERVATION,
            (Supplier<ObjectDataSource>)
                this::createObservationDataSourceWithQuantitiesAndNullSystem,
            "value-quantity",
            List.of("50||local"),
            Set.of("4")), // matches null system with code "local"

        // Prefix with system|code
        Arguments.of(
            "value-quantity gt with system|code",
            ResourceType.OBSERVATION,
            (Supplier<ObjectDataSource>) this::createObservationDataSourceWithQuantities,
            "value-quantity",
            List.of("gt80|http://unitsofmeasure.org|kg"),
            Set.of("2")), // 85.5 > 80

        // Prefix with ||code (any system)
        Arguments.of(
            "value-quantity gt with ||code",
            ResourceType.OBSERVATION,
            (Supplier<ObjectDataSource>) this::createObservationDataSourceWithQuantities,
            "value-quantity",
            List.of("gt80||kg"),
            Set.of("2")), // 85.5 > 80, any system

        // ========== Observation value-quantity UCUM normalization tests ==========
        // UCUM normalization enables matching across equivalent unit representations
        // Data source: obs1=1g, obs2=500mg, obs3=0.5g, obs4=2000mg, obs5=no value

        // Search for 1000 mg -> should match obs1 (1g = 1000mg) and obs4 (2000mg via fallback)
        Arguments.of(
            "value-quantity UCUM: 1000mg matches 1g",
            ResourceType.OBSERVATION,
            (Supplier<ObjectDataSource>) this::createObservationDataSourceForUcumNormalization,
            "value-quantity",
            List.of("1000|http://unitsofmeasure.org|mg"),
            Set.of("1")),

        // Search for 1 g -> should match obs1 (1g) directly
        Arguments.of(
            "value-quantity UCUM: 1g matches 1g",
            ResourceType.OBSERVATION,
            (Supplier<ObjectDataSource>) this::createObservationDataSourceForUcumNormalization,
            "value-quantity",
            List.of("1|http://unitsofmeasure.org|g"),
            Set.of("1")),

        // Search for 500 mg -> should match obs2 (500mg) and obs3 (0.5g = 500mg)
        Arguments.of(
            "value-quantity UCUM: 500mg matches 500mg and 0.5g",
            ResourceType.OBSERVATION,
            (Supplier<ObjectDataSource>) this::createObservationDataSourceForUcumNormalization,
            "value-quantity",
            List.of("500|http://unitsofmeasure.org|mg"),
            Set.of("2", "3")),

        // Search for 0.5 g -> should match obs2 (500mg) and obs3 (0.5g)
        Arguments.of(
            "value-quantity UCUM: 0.5g matches 500mg and 0.5g",
            ResourceType.OBSERVATION,
            (Supplier<ObjectDataSource>) this::createObservationDataSourceForUcumNormalization,
            "value-quantity",
            List.of("0.5|http://unitsofmeasure.org|g"),
            Set.of("2", "3")),

        // Search for 2 g -> should match obs4 (2000mg = 2g)
        Arguments.of(
            "value-quantity UCUM: 2g matches 2000mg",
            ResourceType.OBSERVATION,
            (Supplier<ObjectDataSource>) this::createObservationDataSourceForUcumNormalization,
            "value-quantity",
            List.of("2|http://unitsofmeasure.org|g"),
            Set.of("4")),

        // UCUM with prefix: gt 750 mg -> should match obs1 (1g=1000mg) and obs4 (2000mg)
        Arguments.of(
            "value-quantity UCUM: gt750mg",
            ResourceType.OBSERVATION,
            (Supplier<ObjectDataSource>) this::createObservationDataSourceForUcumNormalization,
            "value-quantity",
            List.of("gt750|http://unitsofmeasure.org|mg"),
            Set.of("1", "4")),

        // UCUM with prefix: lt 1 g -> should match obs2 (500mg) and obs3 (0.5g)
        Arguments.of(
            "value-quantity UCUM: lt1g",
            ResourceType.OBSERVATION,
            (Supplier<ObjectDataSource>) this::createObservationDataSourceForUcumNormalization,
            "value-quantity",
            List.of("lt1|http://unitsofmeasure.org|g"),
            Set.of("2", "3")));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("searchTestCases")
  void testSearch(
      final String testName,
      final ResourceType resourceType,
      final Supplier<ObjectDataSource> dataSourceSupplier,
      final String paramCode,
      final List<String> searchValues,
      final Set<String> expectedIds) {

    final ObjectDataSource dataSource = dataSourceSupplier.get();

    final FhirSearch search =
        FhirSearch.builder().criterion(paramCode, searchValues.toArray(new String[0])).build();

    final FhirSearchExecutor executor =
        FhirSearchExecutor.withRegistry(encoders.getContext(), dataSource, registry);

    final Dataset<Row> results = executor.execute(resourceType, search);

    assertEquals(expectedIds, extractIds(results));
  }

  // ========== Special tests (not parameterized) ==========

  @Test
  void testNoCriteriaReturnsAll() {
    final ObjectDataSource dataSource = createPatientDataSource();

    final FhirSearch search = FhirSearch.builder().build();

    final FhirSearchExecutor executor =
        FhirSearchExecutor.withRegistry(encoders.getContext(), dataSource, registry);

    final Dataset<Row> results = executor.execute(ResourceType.PATIENT, search);

    // Should return all 4 patients
    assertEquals(4, results.count());
  }

  @Test
  void testUnknownParameterThrowsException() {
    final ObjectDataSource dataSource = createPatientDataSource();

    final FhirSearch search = FhirSearch.builder().criterion("unknown-param", "value").build();

    final FhirSearchExecutor executor =
        FhirSearchExecutor.withRegistry(encoders.getContext(), dataSource, registry);

    final UnknownSearchParameterException exception =
        assertThrows(
            UnknownSearchParameterException.class,
            () -> executor.execute(ResourceType.PATIENT, search));

    assertTrue(exception.getMessage().contains("unknown-param"));
    assertTrue(exception.getMessage().contains("Patient"));
  }

  @Test
  void testResultSchemaMatchesOriginal() {
    final ObjectDataSource dataSource = createPatientDataSource();

    final FhirSearch search = FhirSearch.builder().criterion("gender", "male").build();

    final FhirSearchExecutor executor =
        FhirSearchExecutor.withRegistry(encoders.getContext(), dataSource, registry);

    final Dataset<Row> results = executor.execute(ResourceType.PATIENT, search);
    final Dataset<Row> original = dataSource.read("Patient");

    // Schema should be identical
    assertEquals(original.schema(), results.schema());
  }

  @Test
  void testInvalidModifierOnTokenThrowsException() {
    final ObjectDataSource dataSource = createPatientDataSource();

    // :exact is not valid for token type parameters
    final FhirSearch search = FhirSearch.builder().criterion("gender:exact", "male").build();

    final FhirSearchExecutor executor =
        FhirSearchExecutor.withRegistry(encoders.getContext(), dataSource, registry);

    final InvalidModifierException exception =
        assertThrows(
            InvalidModifierException.class, () -> executor.execute(ResourceType.PATIENT, search));

    assertTrue(exception.getMessage().contains("exact"));
    assertTrue(exception.getMessage().contains("TOKEN"));
  }

  @Test
  void testInvalidModifierOnStringThrowsException() {
    final ObjectDataSource dataSource = createPatientDataSourceWithNames();

    // :not is not valid for string type parameters
    final FhirSearch search = FhirSearch.builder().criterion("family:not", "Smith").build();

    final FhirSearchExecutor executor =
        FhirSearchExecutor.withRegistry(encoders.getContext(), dataSource, registry);

    final InvalidModifierException exception =
        assertThrows(
            InvalidModifierException.class, () -> executor.execute(ResourceType.PATIENT, search));

    assertTrue(exception.getMessage().contains("not"));
    assertTrue(exception.getMessage().contains("STRING"));
  }

  @Test
  void testInvalidModifierOnDateThrowsException() {
    final ObjectDataSource dataSource = createPatientDataSourceWithBirthDates();

    // No modifiers are supported for date type parameters in the limited implementation
    final FhirSearch search =
        FhirSearch.builder().criterion("birthdate:exact", "1990-01-15").build();

    final FhirSearchExecutor executor =
        FhirSearchExecutor.withRegistry(encoders.getContext(), dataSource, registry);

    final InvalidModifierException exception =
        assertThrows(
            InvalidModifierException.class, () -> executor.execute(ResourceType.PATIENT, search));

    assertTrue(exception.getMessage().contains("exact"));
    assertTrue(exception.getMessage().contains("DATE"));
  }

  // ========== Factory method tests ==========

  @Test
  void withDefaultRegistry_loadsStandardFhirParameters() {
    // This tests the production factory method with the bundled JSON resource.
    // We use a minimal data source since we're only verifying the defaultRegistry content.
    final ObjectDataSource dataSource = createPatientDataSource();

    final FhirSearchExecutor executor =
        FhirSearchExecutor.withDefaultRegistry(encoders.getContext(), dataSource);

    final SearchParameterRegistry defaultRegistry = executor.getColumnBuilder().getRegistry();

    // Verify a sample of standard FHIR search parameters are loaded.
    // These are stable parameters defined in the FHIR R4 spec.
    assertAll(
        () ->
            assertTrue(
                defaultRegistry.getParameter(ResourceType.PATIENT, "gender").isPresent(),
                "Patient.gender should be present"),
        () ->
            assertTrue(
                defaultRegistry.getParameter(ResourceType.PATIENT, "birthdate").isPresent(),
                "Patient.birthdate should be present"),
        () ->
            assertTrue(
                defaultRegistry.getParameter(ResourceType.OBSERVATION, "code").isPresent(),
                "Observation.code should be present"),
        () ->
            assertTrue(
                defaultRegistry.getParameter(ResourceType.OBSERVATION, "date").isPresent(),
                "Observation.date should be present"));
  }

  // ========== Data source creation methods ==========

  /**
   * Creates a test data source with patients having different name configurations. - Patient 1:
   * name with family "Smith" - Patient 2: two names with families "Jones" and "Johnson" - Patient
   * 3: name with family "Brown" - Patient 4: no name
   */
  private ObjectDataSource createPatientDataSourceWithNames() {
    final Patient patient1 = new Patient();
    patient1.setId("1");
    patient1.addName().setFamily("Smith").addGiven("John");

    final Patient patient2 = new Patient();
    patient2.setId("2");
    patient2.addName().setFamily("Jones").addGiven("Jane");
    patient2.addName().setFamily("Johnson").addGiven("Jane"); // Maiden name

    final Patient patient3 = new Patient();
    patient3.setId("3");
    patient3.addName().setFamily("Brown").addGiven("Bob");

    final Patient patient4 = new Patient();
    patient4.setId("4");
    // No name

    return new ObjectDataSource(spark, encoders, List.of(patient1, patient2, patient3, patient4));
  }

  /**
   * Creates a test data source with patients having different address configurations. - Patient 1:
   * one home address - Patient 2: home and work addresses - Patient 3: temp address - Patient 4: no
   * addresses
   */
  private ObjectDataSource createPatientDataSourceWithAddresses() {
    final Patient patient1 = new Patient();
    patient1.setId("1");
    patient1.addAddress().setUse(AddressUse.HOME).setCity("Sydney");

    final Patient patient2 = new Patient();
    patient2.setId("2");
    patient2.addAddress().setUse(AddressUse.HOME).setCity("Melbourne");
    patient2.addAddress().setUse(AddressUse.WORK).setCity("Brisbane");

    final Patient patient3 = new Patient();
    patient3.setId("3");
    patient3.addAddress().setUse(AddressUse.TEMP).setCity("Perth");

    final Patient patient4 = new Patient();
    patient4.setId("4");
    // No addresses

    return new ObjectDataSource(spark, encoders, List.of(patient1, patient2, patient3, patient4));
  }

  /**
   * Creates a test data source with patients having different birth dates. - Patient 1: birthDate =
   * "1990-01-15" - Patient 2: birthDate = "1985-06-20" - Patient 3: birthDate = "1990-01-15" (same
   * as Patient 1) - Patient 4: no birthDate
   */
  private ObjectDataSource createPatientDataSourceWithBirthDates() {
    final Patient patient1 = new Patient();
    patient1.setId("1");
    patient1.setBirthDateElement(new DateType("1990-01-15"));

    final Patient patient2 = new Patient();
    patient2.setId("2");
    patient2.setBirthDateElement(new DateType("1985-06-20"));

    final Patient patient3 = new Patient();
    patient3.setId("3");
    patient3.setBirthDateElement(new DateType("1990-01-15"));

    final Patient patient4 = new Patient();
    patient4.setId("4");
    // No birthDate set

    return new ObjectDataSource(spark, encoders, List.of(patient1, patient2, patient3, patient4));
  }

  /** Creates a test data source with patients having different genders. */
  private ObjectDataSource createPatientDataSource() {
    final Patient patient1 = new Patient();
    patient1.setId("1");
    patient1.setGender(AdministrativeGender.MALE);

    final Patient patient2 = new Patient();
    patient2.setId("2");
    patient2.setGender(AdministrativeGender.FEMALE);

    final Patient patient3 = new Patient();
    patient3.setId("3");
    patient3.setGender(AdministrativeGender.MALE);

    final Patient patient4 = new Patient();
    patient4.setId("4");
    // No gender set

    return new ObjectDataSource(spark, encoders, List.of(patient1, patient2, patient3, patient4));
  }

  /**
   * Creates a test data source with RiskAssessments having different probabilities. -
   * RiskAssessment 1: probability = 0.2 - RiskAssessment 2: probability = 0.5 - RiskAssessment 3:
   * probability = 0.8 - RiskAssessment 4: no probability
   */
  private ObjectDataSource createRiskAssessmentDataSource() {
    final RiskAssessment ra1 = new RiskAssessment();
    ra1.setId("1");
    final RiskAssessmentPredictionComponent prediction1 = ra1.addPrediction();
    prediction1.setProbability(new DecimalType("0.2"));

    final RiskAssessment ra2 = new RiskAssessment();
    ra2.setId("2");
    final RiskAssessmentPredictionComponent prediction2 = ra2.addPrediction();
    prediction2.setProbability(new DecimalType("0.5"));

    final RiskAssessment ra3 = new RiskAssessment();
    ra3.setId("3");
    final RiskAssessmentPredictionComponent prediction3 = ra3.addPrediction();
    prediction3.setProbability(new DecimalType("0.8"));

    final RiskAssessment ra4 = new RiskAssessment();
    ra4.setId("4");
    // No prediction/probability

    return new ObjectDataSource(spark, encoders, List.of(ra1, ra2, ra3, ra4));
  }

  /**
   * Creates a test data source with Coverage resources having different period configurations. -
   * Coverage 1: period = [2023-01-01, 2023-06-30] (first half of 2023) - Coverage 2: period =
   * [2023-06-01, 2023-12-31] (second half of 2023, overlaps with 1) - Coverage 3: period =
   * [2024-01-01, null] (open-ended future) - Coverage 4: period = [null, 2022-12-31] (open-ended
   * past) - Coverage 5: no period
   */
  private ObjectDataSource createCoverageDataSource() {
    final Coverage coverage1 = new Coverage();
    coverage1.setId("1");
    final Period period1 = new Period();
    period1.setStartElement(new DateTimeType("2023-01-01"));
    period1.setEndElement(new DateTimeType("2023-06-30"));
    coverage1.setPeriod(period1);

    final Coverage coverage2 = new Coverage();
    coverage2.setId("2");
    final Period period2 = new Period();
    period2.setStartElement(new DateTimeType("2023-06-01"));
    period2.setEndElement(new DateTimeType("2023-12-31"));
    coverage2.setPeriod(period2);

    final Coverage coverage3 = new Coverage();
    coverage3.setId("3");
    final Period period3 = new Period();
    period3.setStartElement(new DateTimeType("2024-01-01"));
    // No end = open-ended future (positive infinity)
    coverage3.setPeriod(period3);

    final Coverage coverage4 = new Coverage();
    coverage4.setId("4");
    final Period period4 = new Period();
    // No start = open-ended past (negative infinity)
    period4.setEndElement(new DateTimeType("2022-12-31"));
    coverage4.setPeriod(period4);

    final Coverage coverage5 = new Coverage();
    coverage5.setId("5");
    // No period at all

    return new ObjectDataSource(
        spark, encoders, List.of(coverage1, coverage2, coverage3, coverage4, coverage5));
  }

  /**
   * Creates a test data source with Condition resources having different recordedDate values. Uses
   * dateTime type. - Condition 1: recordedDate = "2023-03-15T10:30:00Z" (full dateTime) - Condition
   * 2: recordedDate = "2023-06-20T14:45:00Z" (different date) - Condition 3: recordedDate =
   * "2023-03-15" (day precision only) - Condition 4: no recordedDate
   */
  private ObjectDataSource createConditionDataSource() {
    final Condition condition1 = new Condition();
    condition1.setId("1");
    condition1.setRecordedDateElement(new DateTimeType("2023-03-15T10:30:00Z"));

    final Condition condition2 = new Condition();
    condition2.setId("2");
    condition2.setRecordedDateElement(new DateTimeType("2023-06-20T14:45:00Z"));

    final Condition condition3 = new Condition();
    condition3.setId("3");
    condition3.setRecordedDateElement(new DateTimeType("2023-03-15"));

    final Condition condition4 = new Condition();
    condition4.setId("4");
    // No recordedDate

    return new ObjectDataSource(
        spark, encoders, List.of(condition1, condition2, condition3, condition4));
  }

  /**
   * Creates a test data source with AuditEvent resources having different recorded values. Uses
   * instant type (always full precision). - AuditEvent 1: recorded = "2023-03-15T10:30:00.123Z" -
   * AuditEvent 2: recorded = "2023-06-20T14:45:30.456Z" - AuditEvent 3: recorded =
   * "2023-03-15T10:30:00.123Z" (duplicate for multi-match) - AuditEvent 4: no recorded (null)
   */
  private ObjectDataSource createAuditEventDataSource() {
    final AuditEvent event1 = new AuditEvent();
    event1.setId("1");
    event1.setRecordedElement(new InstantType("2023-03-15T10:30:00.123Z"));

    final AuditEvent event2 = new AuditEvent();
    event2.setId("2");
    event2.setRecordedElement(new InstantType("2023-06-20T14:45:30.456Z"));

    final AuditEvent event3 = new AuditEvent();
    event3.setId("3");
    event3.setRecordedElement(new InstantType("2023-03-15T10:30:00.123Z"));

    final AuditEvent event4 = new AuditEvent();
    event4.setId("4");
    // No recorded (null)

    return new ObjectDataSource(spark, encoders, List.of(event1, event2, event3, event4));
  }

  /**
   * Creates a test data source with patients having different identifiers. - Patient 1: identifier
   * with system "http://hospital.org/mrn" and value "12345" - Patient 2: identifier with system
   * "http://hospital.org/mrn" and value "67890" - Patient 3: identifier with null system and value
   * "99999" - Patient 4: no identifier
   */
  private ObjectDataSource createPatientDataSourceWithIdentifiers() {
    final Patient patient1 = new Patient();
    patient1.setId("1");
    patient1.addIdentifier().setSystem("http://hospital.org/mrn").setValue("12345");

    final Patient patient2 = new Patient();
    patient2.setId("2");
    patient2.addIdentifier().setSystem("http://hospital.org/mrn").setValue("67890");

    final Patient patient3 = new Patient();
    patient3.setId("3");
    final Identifier identifierNoSystem = new Identifier();
    identifierNoSystem.setValue("99999");
    // No system set
    patient3.addIdentifier(identifierNoSystem);

    final Patient patient4 = new Patient();
    patient4.setId("4");
    // No identifier

    return new ObjectDataSource(spark, encoders, List.of(patient1, patient2, patient3, patient4));
  }

  /**
   * Creates a test data source with patients having different telecoms (ContactPoint). - Patient 1:
   * phone "555-1234" and email "john@example.com" - Patient 2: phone "555-5678" - Patient 3: no
   * telecom
   */
  private ObjectDataSource createPatientDataSourceWithTelecoms() {
    final Patient patient1 = new Patient();
    patient1.setId("1");
    patient1.addTelecom().setSystem(ContactPointSystem.PHONE).setValue("555-1234");
    patient1.addTelecom().setSystem(ContactPointSystem.EMAIL).setValue("john@example.com");

    final Patient patient2 = new Patient();
    patient2.setId("2");
    patient2.addTelecom().setSystem(ContactPointSystem.PHONE).setValue("555-5678");

    final Patient patient3 = new Patient();
    patient3.setId("3");
    // No telecom

    return new ObjectDataSource(spark, encoders, List.of(patient1, patient2, patient3));
  }

  /**
   * Creates a test data source with patients having different active values. - Patient 1: active =
   * true - Patient 2: active = false - Patient 3: active = true - Patient 4: no active value (null)
   */
  private ObjectDataSource createPatientDataSourceWithActive() {
    final Patient patient1 = new Patient();
    patient1.setId("1");
    patient1.setActive(true);

    final Patient patient2 = new Patient();
    patient2.setId("2");
    patient2.setActive(false);

    final Patient patient3 = new Patient();
    patient3.setId("3");
    patient3.setActive(true);

    final Patient patient4 = new Patient();
    patient4.setId("4");
    // No active value

    return new ObjectDataSource(spark, encoders, List.of(patient1, patient2, patient3, patient4));
  }

  /**
   * Creates a test data source with Observations having different effective types. This tests the
   * polymorphic effective[x] field with multiple expression support. - Observation 1:
   * effectiveDateTime = "2023-03-15T10:30:00Z" - Observation 2: effectivePeriod = [2023-06-01,
   * 2023-08-31] - Observation 3: effectiveInstant = "2023-09-15T14:45:00.123Z" - Observation 4: no
   * effective
   */
  private ObjectDataSource createObservationDataSourceWithDates() {
    final Observation obs1 = new Observation();
    obs1.setId("1");
    obs1.setEffective(new DateTimeType("2023-03-15T10:30:00Z"));

    final Observation obs2 = new Observation();
    obs2.setId("2");
    final Period period = new Period();
    period.setStartElement(new DateTimeType("2023-06-01"));
    period.setEndElement(new DateTimeType("2023-08-31"));
    obs2.setEffective(period);

    final Observation obs3 = new Observation();
    obs3.setId("3");
    obs3.setEffective(new InstantType("2023-09-15T14:45:00.123Z"));

    final Observation obs4 = new Observation();
    obs4.setId("4");
    // No effective set

    return new ObjectDataSource(spark, encoders, List.of(obs1, obs2, obs3, obs4));
  }

  /**
   * Creates a test data source with Observations having different Quantity values. - Observation 1:
   * value = 70 kg (body weight) - Observation 2: value = 85.5 kg (body weight) - Observation 3:
   * value = 120 mmHg (blood pressure) - Observation 4: no value
   */
  private ObjectDataSource createObservationDataSourceWithQuantities() {
    final Observation obs1 = new Observation();
    obs1.setId("1");
    obs1.setValue(
        new Quantity()
            .setValue(70)
            .setUnit("kg")
            .setSystem("http://unitsofmeasure.org")
            .setCode("kg"));

    final Observation obs2 = new Observation();
    obs2.setId("2");
    obs2.setValue(
        new Quantity()
            .setValue(85.5)
            .setUnit("kg")
            .setSystem("http://unitsofmeasure.org")
            .setCode("kg"));

    final Observation obs3 = new Observation();
    obs3.setId("3");
    obs3.setValue(
        new Quantity()
            .setValue(120)
            .setUnit("mmHg")
            .setSystem("http://unitsofmeasure.org")
            .setCode("mm[Hg]"));

    final Observation obs4 = new Observation();
    obs4.setId("4");
    // No value

    return new ObjectDataSource(spark, encoders, List.of(obs1, obs2, obs3, obs4));
  }

  /**
   * Creates a test data source with Observations having Quantity values including null system. -
   * Observation 1: value = 70 kg (UCUM) - Observation 2: value = 85.5 kg (UCUM) - Observation 3:
   * value = 120 mmHg (UCUM) - Observation 4: value = 50 (null system, code = "local") - Observation
   * 5: no value
   */
  private ObjectDataSource createObservationDataSourceWithQuantitiesAndNullSystem() {
    final Observation obs1 = new Observation();
    obs1.setId("1");
    obs1.setValue(
        new Quantity()
            .setValue(70)
            .setUnit("kg")
            .setSystem("http://unitsofmeasure.org")
            .setCode("kg"));

    final Observation obs2 = new Observation();
    obs2.setId("2");
    obs2.setValue(
        new Quantity()
            .setValue(85.5)
            .setUnit("kg")
            .setSystem("http://unitsofmeasure.org")
            .setCode("kg"));

    final Observation obs3 = new Observation();
    obs3.setId("3");
    obs3.setValue(
        new Quantity()
            .setValue(120)
            .setUnit("mmHg")
            .setSystem("http://unitsofmeasure.org")
            .setCode("mm[Hg]"));

    final Observation obs4 = new Observation();
    obs4.setId("4");
    // Quantity with null system (local unit)
    final Quantity localQuantity = new Quantity();
    localQuantity.setValue(50);
    localQuantity.setCode("local");
    // No system set
    obs4.setValue(localQuantity);

    final Observation obs5 = new Observation();
    obs5.setId("5");
    // No value

    return new ObjectDataSource(spark, encoders, List.of(obs1, obs2, obs3, obs4, obs5));
  }

  /**
   * Creates a test data source with Observations for UCUM normalization testing. Uses different but
   * equivalent UCUM units to verify normalization works. - Observation 1: value = 1 g - Observation
   * 2: value = 500 mg - Observation 3: value = 0.5 g (equivalent to 500 mg) - Observation 4: value
   * = 2000 mg (equivalent to 2 g) - Observation 5: no value
   */
  private ObjectDataSource createObservationDataSourceForUcumNormalization() {
    final Observation obs1 = new Observation();
    obs1.setId("1");
    obs1.setValue(
        new Quantity()
            .setValue(1)
            .setUnit("g")
            .setSystem("http://unitsofmeasure.org")
            .setCode("g"));

    final Observation obs2 = new Observation();
    obs2.setId("2");
    obs2.setValue(
        new Quantity()
            .setValue(500)
            .setUnit("mg")
            .setSystem("http://unitsofmeasure.org")
            .setCode("mg"));

    final Observation obs3 = new Observation();
    obs3.setId("3");
    obs3.setValue(
        new Quantity()
            .setValue(new BigDecimal("0.5"))
            .setUnit("g")
            .setSystem("http://unitsofmeasure.org")
            .setCode("g"));

    final Observation obs4 = new Observation();
    obs4.setId("4");
    obs4.setValue(
        new Quantity()
            .setValue(2000)
            .setUnit("mg")
            .setSystem("http://unitsofmeasure.org")
            .setCode("mg"));

    final Observation obs5 = new Observation();
    obs5.setId("5");
    // No value

    return new ObjectDataSource(spark, encoders, List.of(obs1, obs2, obs3, obs4, obs5));
  }

  /**
   * Creates a test data source with Observations having different codes (CodeableConcept). -
   * Observation 1: code with LOINC 29463-7 (Body Weight) - Observation 2: code with LOINC 29463-7
   * AND SNOMED 27113001 (multiple codings) - Observation 3: code with null system and code
   * "local-99" - Observation 4: no code
   */
  private ObjectDataSource createObservationDataSource() {
    final Observation obs1 = new Observation();
    obs1.setId("1");
    obs1.setCode(
        new CodeableConcept()
            .addCoding(
                new Coding()
                    .setSystem("http://loinc.org")
                    .setCode("29463-7")
                    .setDisplay("Body Weight")));

    final Observation obs2 = new Observation();
    obs2.setId("2");
    obs2.setCode(
        new CodeableConcept()
            .addCoding(
                new Coding()
                    .setSystem("http://loinc.org")
                    .setCode("29463-7")
                    .setDisplay("Body Weight"))
            .addCoding(
                new Coding()
                    .setSystem("http://snomed.info/sct")
                    .setCode("27113001")
                    .setDisplay("Body weight")));

    final Observation obs3 = new Observation();
    obs3.setId("3");
    final Coding localCoding = new Coding();
    localCoding.setCode("local-99");
    // No system set
    obs3.setCode(new CodeableConcept().addCoding(localCoding));

    final Observation obs4 = new Observation();
    obs4.setId("4");
    // No code

    return new ObjectDataSource(spark, encoders, List.of(obs1, obs2, obs3, obs4));
  }

  /** Extracts resource IDs from a result dataset. */
  private Set<String> extractIds(final Dataset<Row> results) {
    return results.select("id").collectAsList().stream()
        .map(row -> row.getString(0))
        .collect(Collectors.toSet());
  }
}
