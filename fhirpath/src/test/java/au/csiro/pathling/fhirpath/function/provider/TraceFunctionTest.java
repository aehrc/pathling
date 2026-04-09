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

package au.csiro.pathling.fhirpath.function.provider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.ListTraceCollector;
import au.csiro.pathling.fhirpath.ListTraceCollector.TraceEntry;
import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.collection.StringCollection;
import au.csiro.pathling.fhirpath.evaluation.CollectionDataset;
import au.csiro.pathling.fhirpath.evaluation.CrossResourceStrategy;
import au.csiro.pathling.fhirpath.evaluation.DatasetEvaluator;
import au.csiro.pathling.fhirpath.evaluation.DatasetEvaluatorBuilder;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.sql.TraceExpression;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.assertions.Assertions;
import au.csiro.pathling.test.datasource.ObjectDataSource;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import jakarta.annotation.Nonnull;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.AdministrativeGender;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.HumanName;
import org.hl7.fhir.r4.model.Patient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

/** Tests for the FHIRPath trace() function. */
@SpringBootUnitTest
class TraceFunctionTest {

  private static final Parser PARSER = new Parser();

  @Autowired SparkSession spark;

  @Autowired FhirEncoders encoders;

  private DatasetEvaluator evaluator;
  private ListAppender<ILoggingEvent> logAppender;

  @BeforeEach
  void setUp() {
    final ObjectDataSource dataSource =
        new ObjectDataSource(
            spark, encoders, List.of(createPatient1(), createPatient2(), createPatient3()));
    final Dataset<Row> dataset = dataSource.read("Patient");

    evaluator =
        DatasetEvaluatorBuilder.create(ResourceType.PATIENT, encoders.getContext())
            .withDataset(dataset)
            .withCrossResourceStrategy(CrossResourceStrategy.EMPTY)
            .build();

    // Attach a ListAppender to capture TraceExpression log output.
    logAppender = new ListAppender<>();
    logAppender.start();
    getTraceLogger().addAppender(logAppender);
  }

  @AfterEach
  void tearDown() {
    getTraceLogger().detachAppender(logAppender);
    logAppender.stop();
  }

  @Nonnull
  private static Logger getTraceLogger() {
    return (Logger) LoggerFactory.getLogger(TraceExpression.class);
  }

  @Nonnull
  private CollectionDataset evaluate(@Nonnull final String expression) {
    final FhirPath fhirPath = PARSER.parse(expression);
    return evaluator.evaluate(fhirPath);
  }

  @Nested
  class PassThroughTests {

    @Test
    void trace_boolean_returnsSameValues() {
      Assertions.assertThat(evaluate("Patient.active.trace('active-check')"))
          .hasClass(BooleanCollection.class)
          .toCanonicalResult()
          .hasRowsUnordered(
              RowFactory.create("1", true),
              RowFactory.create("2", false),
              RowFactory.create("3", null));
    }

    @Test
    void trace_string_returnsSameValues() {
      Assertions.assertThat(evaluate("Patient.gender.trace('debug')"))
          .hasClass(StringCollection.class)
          .toCanonicalResult()
          .hasRowsUnordered(
              RowFactory.create("1", "female"),
              RowFactory.create("2", "male"),
              RowFactory.create("3", null));
    }

    @Test
    void trace_complexType_returnsSameValues() {
      assertTraceIsPassThrough("Patient.name", "Patient.name.trace('names')");
    }

    @Test
    void trace_afterWhere_returnsSameValues() {
      assertTraceIsPassThrough(
          "Patient.name.where(use = 'official')",
          "Patient.name.where(use = 'official').trace('official-names')");
    }

    @Test
    void trace_beforeWhere_returnsSameValues() {
      assertTraceIsPassThrough(
          "Patient.name.where(use = 'official')",
          "Patient.name.trace('all-names').where(use = 'official')");
    }

    @Test
    void trace_twoTraceCalls_returnsSameValues() {
      assertTraceIsPassThrough(
          "Patient.name.where(use = 'official')",
          "Patient.name.trace('before-filter').where(use = 'official')" + ".trace('after-filter')");
    }

    @Test
    void trace_multiElementCollection_returnsSameValues() {
      assertTraceIsPassThrough("Patient.name.given", "Patient.name.given.trace('givens')");
    }

    private void assertTraceIsPassThrough(
        @Nonnull final String baseExpression, @Nonnull final String tracedExpression) {
      final CollectionDataset expected = evaluate(baseExpression);
      final CollectionDataset actual = evaluate(tracedExpression);

      // Verify type equality.
      assertEquals(
          expected.getValue().getFhirType().get().toCode(),
          actual.getValue().getFhirType().get().toCode());

      // Verify value equality.
      final Dataset<Row> expectedDs = expected.toCanonical().toIdValueDataset();
      final Dataset<Row> actualDs = actual.toCanonical().toIdValueDataset();
      Assertions.assertThat(actualDs).hasRowsUnordered(expectedDs);
    }

    @Test
    void trace_emptyCollection_returnsEmpty() {
      final CollectionDataset result = evaluate("{}.trace('empty')");
      assertNotNull(result);
    }

    @Test
    void trace_withProjection_returnsInputUnchanged() {
      assertTraceIsPassThrough("Patient.name", "Patient.name.trace('fam', family)");
    }

    @Test
    void trace_withProjection_complexExpression_returnsInputUnchanged() {
      assertTraceIsPassThrough(
          "Patient.name", "Patient.name.trace('full', given.first() + ' ' + family)");
    }

    @Test
    void trace_withProjection_returningNull_returnsInputUnchanged() {
      // Patient.name.text is null for all test patients; projection produces null but input is
      // returned unchanged.
      assertTraceIsPassThrough("Patient.name", "Patient.name.trace('missing', text)");
    }

    @Test
    void trace_withProjection_onEmptyCollection_returnsEmpty() {
      final CollectionDataset result = evaluate("{}.trace('empty', id)");
      assertNotNull(result);
    }
  }

  @Nested
  class LoggingTests {

    @Test
    void trace_logsWithLabel() {
      // Materialise the result to trigger expression evaluation.
      evaluate("Patient.active.trace('myLabel')").toCanonical().toIdValueDataset().collectAsList();

      final boolean hasLabelledEntry =
          logAppender.list.stream()
              .anyMatch(event -> event.getFormattedMessage().contains("myLabel"));
      assertTrue(hasLabelledEntry, "Expected log entry containing 'myLabel'");
    }

    @Test
    void trace_logsValueRepresentation() {
      evaluate("Patient.name.family.trace('names')")
          .toCanonical()
          .toIdValueDataset()
          .collectAsList();

      final boolean hasSmith =
          logAppender.list.stream()
              .anyMatch(event -> event.getFormattedMessage().contains("Smith"));
      assertTrue(hasSmith, "Expected log entry containing 'Smith'");
    }

    @Test
    void trace_withProjection_complexExpression_logsProjectedValue() {
      evaluate("Patient.name.trace('full', given.first() + ' ' + family)")
          .toCanonical()
          .toIdValueDataset()
          .collectAsList();

      final boolean hasJaneSmith =
          logAppender.list.stream()
              .anyMatch(event -> event.getFormattedMessage().contains("Jane Smith"));
      assertTrue(hasJaneSmith, "Expected log entry containing concatenated 'Jane Smith'");
    }

    @Test
    void trace_withProjection_returningNull_doesNotLog() {
      // When the projection evaluates to an empty collection, the normalised column is null, so no
      // trace entry is produced.
      evaluate("Patient.name.trace('missing', text)")
          .toCanonical()
          .toIdValueDataset()
          .collectAsList();

      final boolean hasMissingLabel =
          logAppender.list.stream()
              .anyMatch(event -> event.getFormattedMessage().contains("[trace:missing]"));
      assertFalse(hasMissingLabel, "Expected no log entries when projection evaluates to null");
    }

    @Test
    void trace_withProjection_logsProjectedValue() {
      evaluate("Patient.name.trace('fam', family)")
          .toCanonical()
          .toIdValueDataset()
          .collectAsList();

      final boolean hasSmith =
          logAppender.list.stream()
              .anyMatch(event -> event.getFormattedMessage().contains("Smith"));
      assertTrue(hasSmith, "Expected log entry containing projected value 'Smith'");
    }

    @Test
    void trace_twoTraceCalls_produceDistinctLogEntries() {
      evaluate("Patient.name.trace('before').where(use = 'official').trace('after').family")
          .toCanonical()
          .toIdValueDataset()
          .collectAsList();

      final boolean hasBefore =
          logAppender.list.stream()
              .anyMatch(event -> event.getFormattedMessage().contains("[trace:before]"));
      final boolean hasAfter =
          logAppender.list.stream()
              .anyMatch(event -> event.getFormattedMessage().contains("[trace:after]"));
      assertTrue(hasBefore, "Expected log entry labelled 'before'");
      assertTrue(hasAfter, "Expected log entry labelled 'after'");
    }
  }

  @Nested
  class ErrorTests {

    @Test
    void trace_withNoArguments_raisesError() {
      // The error may occur at parse time or evaluation time.
      assertThrows(Exception.class, () -> evaluate("Patient.active.trace()"));
    }
  }

  @Nested
  class CollectorTests {

    private ListTraceCollector collector;
    private DatasetEvaluator collectorEvaluator;

    @BeforeEach
    void setUpCollector() {
      collector = new ListTraceCollector();
      final ObjectDataSource dataSource =
          new ObjectDataSource(
              spark, encoders, List.of(createPatient1(), createPatient2(), createPatient3()));
      final Dataset<Row> dataset = dataSource.read("Patient");
      collectorEvaluator =
          DatasetEvaluatorBuilder.create(ResourceType.PATIENT, encoders.getContext())
              .withDataset(dataset)
              .withCrossResourceStrategy(CrossResourceStrategy.EMPTY)
              .withTraceCollector(collector)
              .build();
    }

    private void materialize(@Nonnull final String expression) {
      final FhirPath fhirPath = PARSER.parse(expression);
      collectorEvaluator.evaluate(fhirPath).toCanonical().toIdValueDataset().collectAsList();
    }

    @Test
    void collector_capturesEntriesWithLabel() {
      materialize("Patient.active.trace('myLabel')");

      final List<TraceEntry> entries = collector.getEntries();
      assertFalse(entries.isEmpty());
      assertTrue(entries.stream().allMatch(e -> "myLabel".equals(e.label())));
    }

    @Test
    void collector_capturesCorrectFhirType_primitive() {
      materialize("Patient.active.trace('flag')");

      final List<TraceEntry> entries = collector.getEntries();
      assertFalse(entries.isEmpty());
      assertTrue(entries.stream().allMatch(e -> "boolean".equals(e.fhirType())));
    }

    @Test
    void collector_capturesCorrectFhirType_complex() {
      materialize("Patient.name.trace('names')");

      final List<TraceEntry> entries = collector.getEntries();
      assertFalse(entries.isEmpty());
      assertTrue(entries.stream().allMatch(e -> "HumanName".equals(e.fhirType())));
    }

    @Test
    void collector_twoTraceCallsProduceDistinctLabels() {
      materialize("Patient.name.trace('a').where(use = 'official').trace('b').family");

      final List<TraceEntry> entries = collector.getEntries();
      assertTrue(entries.stream().anyMatch(e -> "a".equals(e.label())));
      assertTrue(entries.stream().anyMatch(e -> "b".equals(e.label())));
    }

    @Test
    void collector_withProjection_capturesProjectedFhirType() {
      materialize("Patient.name.trace('fam', family)");

      final List<TraceEntry> entries = collector.getEntries();
      assertFalse(entries.isEmpty());
      assertTrue(
          entries.stream().allMatch(e -> "string".equals(e.fhirType())),
          "Expected projected FHIR type 'string', not input type 'HumanName'");
      assertTrue(entries.stream().allMatch(e -> "fam".equals(e.label())));
    }

    @Test
    void collector_chainedTrace_innerNotDoubleEvaluated() {
      // When trace('inner').trace('outer') is evaluated without a projection, the outer
      // TraceExpression has the same expression for both left and right children. This test
      // verifies
      // that the inner trace fires exactly once per element, not twice due to double evaluation.
      materialize("Patient.name.trace('inner').trace('outer')");

      final long innerCount =
          collector.getEntries().stream().filter(e -> "inner".equals(e.label())).count();
      final long outerCount =
          collector.getEntries().stream().filter(e -> "outer".equals(e.label())).count();
      assertEquals(
          outerCount,
          innerCount,
          "Inner trace should fire the same number of times as outer trace, not double");
    }

    @Test
    void evaluationWithoutCollector_stillWorks() {
      // The default evaluator has no collector; evaluation should succeed with SLF4J only.
      final CollectionDataset result = evaluate("Patient.active.trace('test')");
      assertNotNull(result);
    }
  }

  private static Patient createPatient1() {
    final Patient p = new Patient();
    p.setId("Patient/1");
    p.setGender(AdministrativeGender.FEMALE);
    p.setActive(true);
    p.addName().setUse(HumanName.NameUse.OFFICIAL).setFamily("Smith").addGiven("Jane");
    return p;
  }

  private static Patient createPatient2() {
    final Patient p = new Patient();
    p.setId("Patient/2");
    p.setGender(AdministrativeGender.MALE);
    p.setActive(false);
    p.addName()
        .setUse(HumanName.NameUse.NICKNAME)
        .setFamily("Doe")
        .addGiven("John")
        .addGiven("James");
    return p;
  }

  private static Patient createPatient3() {
    final Patient p = new Patient();
    p.setId("Patient/3");
    return p;
  }
}
