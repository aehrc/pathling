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

package au.csiro.pathling.search;

import static au.csiro.pathling.search.SearchParameterType.QUANTITY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.datasource.ObjectDataSource;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Age;
import org.hl7.fhir.r4.model.CarePlan;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.DateTimeType;
import org.hl7.fhir.r4.model.DiagnosticReport;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.Immunization;
import org.hl7.fhir.r4.model.InstantType;
import org.hl7.fhir.r4.model.MessageHeader;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Period;
import org.hl7.fhir.r4.model.Procedure;
import org.hl7.fhir.r4.model.Quantity;
import org.hl7.fhir.r4.model.Range;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.Timing;
import org.hl7.fhir.r4.model.Type;
import org.hl7.fhir.r4.model.UriType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Tests a search whose parameter expression is a choice element, such as {@code
 * Observation.effective}, through the default registry.
 *
 * <p>Each alternative of the choice that the parameter type allows is searched, and a resource
 * matches if any of them does, or with the {@code :not} modifier if none of them does. An
 * alternative that the parameter type does not allow, such as a {@code Timing} or a {@code string}
 * for a date parameter, is skipped, so a resource that holds only that alternative does not match.
 *
 * @author John Grimes
 */
@SpringBootUnitTest
@TestInstance(Lifecycle.PER_CLASS)
class ChoiceElementSearchTest {

  @Autowired SparkSession spark;

  @Autowired FhirEncoders encoders;

  private ObjectDataSource dataSource;

  @BeforeAll
  void setUp() {
    dataSource =
        new ObjectDataSource(
            spark,
            encoders,
            List.of(
                // Observation.effective[x]: dateTime, Period, Timing and instant.
                observation("dateTime", new DateTimeType("2020-03-01")),
                observation("instant", new InstantType("2020-06-15T10:00:00Z")),
                observation("period", period("2020-02-01", "2020-02-10")),
                observation("timing", timing("2020-05-01")),
                observation("later", new DateTimeType("2021-02-01")),
                observation("none", null),
                // DiagnosticReport.effective[x]: dateTime and Period.
                diagnosticReport("dateTime", new DateTimeType("2020-03-01")),
                diagnosticReport("period", period("2020-02-01", "2020-02-10")),
                diagnosticReport("earlier", new DateTimeType("2019-01-01")),
                // Immunization.occurrence[x]: dateTime and string.
                immunization("dateTime", new DateTimeType("2020-03-01")),
                immunization("string", new StringType("2020")),
                // Procedure.performed[x]: dateTime, Period, string, Age and Range.
                procedure("dateTime", new DateTimeType("2020-03-01")),
                procedure("period", period("2020-02-01", "2020-02-10")),
                procedure("string", new StringType("2020")),
                procedure("age", (Age) new Age().setValue(40).setUnit("a")),
                procedure("range", new Range().setLow(new Quantity(1)).setHigh(new Quantity(2))),
                // MessageHeader.event[x]: Coding and uri.
                messageHeader("coding", new Coding("http://s", "c1", null)),
                messageHeader("otherCoding", new Coding("http://s", "c2", null)),
                messageHeader("uri", new UriType("http://s/c1")),
                messageHeader("none", null),
                // CarePlan.activity.detail.scheduled[x], under a repeating element: Timing, Period
                // and string.
                carePlan(
                    "twoYears",
                    period("2019-03-01", "2019-03-10"),
                    period("2020-03-01", "2020-03-10")),
                carePlan("timing", timing("2020-05-01")),
                carePlan("string", new StringType("2020"))));
  }

  @Nonnull
  Stream<Arguments> searchCases() {
    return Stream.of(
        // The dateTime, instant and Period alternatives are each matched, and the Timing one is
        // skipped.
        arguments(ResourceType.OBSERVATION, "date=2020", List.of("dateTime", "instant", "period")),
        arguments(
            ResourceType.OBSERVATION,
            "date=ge2020-01-01",
            List.of("dateTime", "instant", "period", "later")),
        arguments(ResourceType.OBSERVATION, "date=lt2020-02-15", List.of("period")),
        arguments(ResourceType.OBSERVATION, "date=2021", List.of("later")),
        arguments(
            ResourceType.OBSERVATION,
            "date=2020,2021",
            List.of("dateTime", "instant", "period", "later")),
        arguments(ResourceType.OBSERVATION, "date=1999", List.of()),
        arguments(ResourceType.DIAGNOSTICREPORT, "date=2020", List.of("dateTime", "period")),
        arguments(ResourceType.DIAGNOSTICREPORT, "date=lt2020-01-01", List.of("earlier")),
        // The string alternative is skipped.
        arguments(ResourceType.IMMUNIZATION, "date=2020", List.of("dateTime")),
        // The string, Age and Range alternatives are skipped.
        arguments(ResourceType.PROCEDURE, "date=2020", List.of("dateTime", "period")),
        // A token search matches the Coding alternative by its code and the uri alternative by
        // its value.
        arguments(ResourceType.MESSAGEHEADER, "event=c1", List.of("coding")),
        arguments(ResourceType.MESSAGEHEADER, "event=http://s/c1", List.of("uri")),
        // A negated search matches a resource only if no alternative holds the value, including
        // a resource with no value at all.
        arguments(
            ResourceType.MESSAGEHEADER, "event:not=c1", List.of("otherCoding", "uri", "none")),
        arguments(
            ResourceType.MESSAGEHEADER,
            "event:not=http://s/c1",
            List.of("coding", "otherCoding", "none")),
        // Each repetition of the element holding the choice is searched, and the Timing and string
        // alternatives are skipped.
        arguments(ResourceType.CAREPLAN, "activity-date=2019", List.of("twoYears")),
        arguments(ResourceType.CAREPLAN, "activity-date=2020", List.of("twoYears")));
  }

  @ParameterizedTest(name = "{0}?{1}")
  @MethodSource("searchCases")
  void searchMatchesEachAllowedAlternativeOfChoice(
      @Nonnull final ResourceType resourceType,
      @Nonnull final String query,
      @Nonnull final List<String> expectedIds) {
    final FhirSearchExecutor executor =
        FhirSearchExecutor.withDefaultRegistry(encoders.getContext(), dataSource);

    final Dataset<Row> results = executor.execute(resourceType, FhirSearch.fromQueryString(query));

    assertThat(results.select("id").collectAsList().stream().map(row -> row.getString(0)))
        .containsExactlyInAnyOrderElementsOf(expectedIds);
  }

  @Test
  void choiceWithNoAlternativeAllowedByParameterTypeIsRejected() {
    // No alternative of Observation.effective is a Quantity, so nothing can be searched.
    final SearchParameterRegistry registry =
        new SearchParameterRegistry(
            Map.of(
                "Observation",
                Map.of(
                    "effective-quantity",
                    new SearchParameterDefinition(
                        "effective-quantity", QUANTITY, "Observation.effective"))));
    final FhirSearchExecutor executor =
        FhirSearchExecutor.withRegistry(encoders.getContext(), dataSource, registry);
    final FhirSearch search = FhirSearch.builder().criterion("effective-quantity", "1").build();

    assertThatThrownBy(() -> executor.execute(ResourceType.OBSERVATION, search))
        .isInstanceOf(InvalidSearchParameterException.class)
        .hasMessageContaining("Observation.effective")
        .hasMessageContaining("QUANTITY");
  }

  @Nonnull
  private static Observation observation(@Nonnull final String id, @Nullable final Type effective) {
    final Observation observation = new Observation();
    observation.setId(id);
    observation.setEffective(effective);
    return observation;
  }

  @Nonnull
  private static DiagnosticReport diagnosticReport(
      @Nonnull final String id, @Nonnull final Type effective) {
    final DiagnosticReport report = new DiagnosticReport();
    report.setId(id);
    report.setEffective(effective);
    return report;
  }

  @Nonnull
  private static Immunization immunization(
      @Nonnull final String id, @Nonnull final Type occurrence) {
    final Immunization immunization = new Immunization();
    immunization.setId(id);
    immunization.setOccurrence(occurrence);
    return immunization;
  }

  @Nonnull
  private static Procedure procedure(@Nonnull final String id, @Nonnull final Type performed) {
    final Procedure procedure = new Procedure();
    procedure.setId(id);
    procedure.setPerformed(performed);
    return procedure;
  }

  @Nonnull
  private static MessageHeader messageHeader(@Nonnull final String id, @Nullable final Type event) {
    final MessageHeader header = new MessageHeader();
    header.setId(id);
    header.setEvent(event);
    return header;
  }

  @Nonnull
  private static CarePlan carePlan(@Nonnull final String id, @Nonnull final Type... scheduled) {
    final CarePlan carePlan = new CarePlan();
    carePlan.setId(id);
    for (final Type value : scheduled) {
      carePlan.addActivity().getDetail().setScheduled(value);
    }
    return carePlan;
  }

  @Nonnull
  private static Period period(@Nonnull final String start, @Nonnull final String end) {
    return new Period()
        .setStartElement(new DateTimeType(start))
        .setEndElement(new DateTimeType(end));
  }

  @Nonnull
  private static Timing timing(@Nonnull final String event) {
    final Timing timing = new Timing();
    timing.addEventElement().setValueAsString(event);
    return timing;
  }
}
