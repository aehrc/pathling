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
import au.csiro.pathling.fhirpath.evaluation.SingleInstanceEvaluationResult;
import au.csiro.pathling.fhirpath.evaluation.SingleInstanceEvaluator;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.sql.TraceExpression;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.assertions.Assertions;
import au.csiro.pathling.test.datasource.ObjectDataSource;
import ca.uhn.fhir.context.FhirContext;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.AdministrativeGender;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.HumanName;
import org.hl7.fhir.r4.model.Patient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
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

  /**
   * Tests that exercise the trace-entry duplication scenarios from issue #2594. Downstream FHIRPath
   * operations whose Spark column form references the traced operand more than once historically
   * inflated the collector entry count: a single source-level {@code trace('t')} fired twice (or
   * more) per row when consumed by {@code count()}, {@code exists()}, {@code empty()}, {@code
   * last()}, {@code combine()}, or the {@code |} union operator. The fix in this change rewrites
   * the offending {@link au.csiro.pathling.fhirpath.column.ColumnRepresentation} methods so each
   * traced operand is evaluated exactly once per logical invocation.
   *
   * <p>These tests use {@link SingleInstanceEvaluator} — the evaluation path used by the FHIRPath
   * Lab API — because that is where the bug was originally observed. The fixture is a single
   * Patient with three {@code name} entries, matching the reproduction in the issue.
   */
  @Nested
  class TraceEntryCountTest {

    private Dataset<Row> patientDf;
    private FhirContext fhirContext;

    @BeforeEach
    void setUpSingleInstance() {
      final ObjectDataSource dataSource =
          new ObjectDataSource(spark, encoders, List.of(createPatientWithThreeNames()));
      patientDf = dataSource.read("Patient");
      fhirContext = encoders.getContext();
    }

    private long countTraceValues(@Nonnull final String expression, @Nonnull final String label) {
      final SingleInstanceEvaluationResult result =
          SingleInstanceEvaluator.evaluate(
              patientDf, "Patient", fhirContext, expression, null, null);
      return result.getTraces().stream()
          .filter(t -> label.equals(t.getLabel()))
          .mapToLong(t -> t.getValues().size())
          .sum();
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("entryCountCases")
    void entryCount(final TraceEntryCase testCase) {
      assertEntryCount(testCase);
    }

    private void assertEntryCount(@Nonnull final TraceEntryCase testCase) {
      final long actual = countTraceValues(testCase.expression(), testCase.label());
      assertEquals(
          testCase.expected(),
          actual,
          () ->
              String.format(
                  "Expression [%s]: expected %d trace entries for label '%s', got %d. "
                      + "See issue #2594.",
                  testCase.expression(), testCase.expected(), testCase.label(), actual));
    }

    static Stream<Arguments> entryCountCases() {
      // The full matrix from issue #2594, including operations that previously inflated the
      // trace count via multi-reference Spark patterns (count, exists, empty, combine, union).
      // After the fix in this change, each source-level trace() call fires exactly once per
      // row regardless of how the result is consumed downstream.
      return Stream.of(
          // Pass-through and non-duplicating cases — regression guard for any fix.
          Arguments.of(new TraceEntryCase("Patient.name.trace('t')", "t", 3)),
          Arguments.of(new TraceEntryCase("Patient.name.trace('t').given.join(' ')", "t", 3)),
          Arguments.of(new TraceEntryCase("Patient.name.trace('t').given.join(' ') + 'X'", "t", 3)),
          Arguments.of(new TraceEntryCase("Patient.name.trace('t').first()", "t", 3)),
          // Previously known-failing rows — the rewrites in this change retire the bug.
          Arguments.of(new TraceEntryCase("Patient.name.trace('t').given.count()", "t", 3)),
          Arguments.of(new TraceEntryCase("Patient.name.trace('t').exists()", "t", 3)),
          Arguments.of(new TraceEntryCase("Patient.name.trace('t').empty()", "t", 3)),
          Arguments.of(
              new TraceEntryCase("Patient.name.trace('t').given.join(' ').combine('X')", "t", 3)),
          Arguments.of(
              new TraceEntryCase(
                  "Patient.name.trace('t').given.join(' ') | Patient.name.family.first()", "t", 3)),
          Arguments.of(
              new TraceEntryCase("Patient.name.trace('t') | Patient.name.trace('t')", "t", 6)),
          // Additional FHIRPath surface (D4 in the design) — extends user-visible regression
          // coverage to a count comparison and two extra downstream pipelines that route through
          // the rewritten ColumnRepresentation methods. The original D4 list also named single()
          // and iif(); neither is implemented in Pathling, so they are replaced with equivalent
          // pipelines that exercise the same internal helpers (singular() via ensureSingular()
          // through .first(), and conditional projection through .where()).
          Arguments.of(new TraceEntryCase("Patient.name.trace('t').given.count() > 0", "t", 3)),
          Arguments.of(
              new TraceEntryCase(
                  "Patient.name.trace('t').where(use = 'official').given.first()", "t", 3)),
          Arguments.of(
              new TraceEntryCase(
                  "Patient.name.trace('t').given.combine(Patient.name.family)", "t", 3)),
          // BooleanOperator XOR — leftValue referenced 3× in the XOR switch arm (isNull,
          // equalTo(true), equalTo(false)), so a traced left operand fires 3× without the
          // binaryOperator let()-wrapping fix.
          Arguments.of(
              new TraceEntryCase(
                  "Patient.name.exists().trace('t') xor Patient.name.exists()", "t", 1)),
          // BooleanOperator IMPLIES with a false left — leftValue referenced 2× (equalTo(true)
          // then equalTo(false)), so a traced left operand fires 2× without the fix.
          Arguments.of(new TraceEntryCase("Patient.name.empty().trace('t') implies true", "t", 1)),
          // BooleanOperator IMPLIES with a traced right operand — rightValue appears in both the
          // lv==true branch and the otherwise sub-when. Without let()-wrapping a traced right
          // operand fires 2× per row when the left is null or true.
          Arguments.of(new TraceEntryCase("true implies 'true'.trace('t').toBoolean()", "t", 1)),
          // EqualityOperator = — left ColumnRepresentation is read via isEmpty(), count(), and
          // singular(), each independently calling getValue(). Without let()-wrapping in
          // handleEquivalentTypes, a traced left operand fires 3× per row.
          Arguments.of(
              new TraceEntryCase("Patient.name.family.first().trace('t') = 'Smith'", "t", 1)),
          // ConversionLogic.convertToBoolean (STRING path) — value appears in both when()
          // predicates ('1.0' and '0.0' checks) and the otherwise() branch. Without let()-wrapping,
          // a traced operand fires 3× per row (all three predicates/branches evaluate value).
          Arguments.of(new TraceEntryCase("'true'.trace('t').toBoolean()", "t", 1)),
          // ConversionLogic.convertToInteger (STRING path) — value appears in both the when()
          // predicate (rlike check) and the value branch (try_cast). Without let()-wrapping, a
          // traced operand fires 2× per row when the input matches the integer regex.
          Arguments.of(new TraceEntryCase("'1'.trace('t').toInteger()", "t", 1)),
          // ConversionLogic.convertToDate (STRING path) — value appears in both the when()
          // predicate (rlike check) and the value branch (the date string itself). Without
          // let()-wrapping, a traced operand fires 2× per row when the input matches the date
          // regex.
          Arguments.of(new TraceEntryCase("'2020-01-01'.trace('t').toDate()", "t", 1)),
          // ConversionLogic.convertToDateTime (STRING path) — structurally identical to
          // convertToDate: value appears in both the when() predicate and value branch.
          Arguments.of(
              new TraceEntryCase("'2020-01-01T12:00:00Z'.trace('t').toDateTime()", "t", 1)),
          // ConversionLogic.convertToTime (STRING path) — structurally identical to
          // convertToDate: value appears in both the when() predicate and value branch.
          Arguments.of(new TraceEntryCase("'10:30:00'.trace('t').toTime()", "t", 1)),
          // QuantityEncoding.encodeNumeric (via convertToQuantity INTEGER path) — the traced input
          // appears in both the when() predicate (isNotNull check) and the value struct (via cast).
          // let()-wrapping on the raw numericColumn ensures the non-deterministic expression is
          // materialized once before both uses.
          Arguments.of(new TraceEntryCase("1.trace('t').toQuantity()", "t", 1)),
          // QuantityValue.toUnit() — quantityColumn is referenced 5× in the assembled
          // when().otherwise() expression (literal.unit, isUcum, isCalendarDuration, callUDF,
          // and the value branch). Without let()-wrapping, a traced Quantity fires 5× per row.
          Arguments.of(new TraceEntryCase("1.toQuantity().trace('t').toQuantity('1')", "t", 1)),
          // QuantityValue.convertibleToUnit() — quantityColumn is referenced 5× similarly
          // (literal.unit, isUcum, isCalendarDuration, callUDF, and quantityColumn.isNotNull).
          // Without let()-wrapping, a traced Quantity fires 5× per row.
          Arguments.of(
              new TraceEntryCase("1.toQuantity().trace('t').convertsToQuantity('1')", "t", 1)));
    }

    @Test
    void codingUnion_traceSingleFire() {
      // SqlFunctions.arrayDistinctWithEquality() referenced `arrayColumn` twice — once for
      // filter() to build the empty-typed seed, and once for aggregate(). For Coding (which uses
      // CodingEquality rather than default SQL equality) both union paths route through this
      // method, so a traced coding array fired 2× per row before the let()-wrap fix.
      final ObjectDataSource ds =
          new ObjectDataSource(spark, encoders, List.of(createPatientWithMaritalStatusCoding()));
      final Dataset<Row> codingDf = ds.read("Patient");

      // handleOneEmpty path: right side is EmptyCollection → dedupeArray →
      // arrayDistinctWithEquality
      final SingleInstanceEvaluationResult emptyUnion =
          SingleInstanceEvaluator.evaluate(
              codingDf,
              "Patient",
              fhirContext,
              "Patient.maritalStatus.coding.trace('t') | {}",
              null,
              null);
      final long emptyUnionCount =
          emptyUnion.getTraces().stream()
              .filter(t -> "t".equals(t.getLabel()))
              .mapToLong(t -> t.getValues().size())
              .sum();
      assertEquals(
          1,
          emptyUnionCount,
          "Trace in Coding union (handleOneEmpty → dedupeArray → arrayDistinctWithEquality)"
              + " should fire exactly once. See issue #2594.");

      // handleEquivalentTypes path: both sides non-empty → unionArrays → arrayDistinctWithEquality
      final SingleInstanceEvaluationResult twoSideUnion =
          SingleInstanceEvaluator.evaluate(
              codingDf,
              "Patient",
              fhirContext,
              "Patient.maritalStatus.coding.trace('t') | Patient.maritalStatus.coding",
              null,
              null);
      final long twoSideCount =
          twoSideUnion.getTraces().stream()
              .filter(t -> "t".equals(t.getLabel()))
              .mapToLong(t -> t.getValues().size())
              .sum();
      assertEquals(
          1,
          twoSideCount,
          "Trace in Coding union (handleEquivalentTypes → unionArrays → arrayDistinctWithEquality)"
              + " should fire exactly once. See issue #2594.");
    }

    @Test
    void codingEquality_traceSingleFire() {
      // Coding equality routes through CodingEquality.equalsTo, which references the left
      // operand once for the null check and once per equality field (5 fields), for a total of
      // 6 references — on top of the 2 from EqualityOperator (isEmpty + count). Without
      // let()-wrapping in handleEquivalentTypes, a traced Coding fires up to 8× per row.
      final ObjectDataSource ds =
          new ObjectDataSource(spark, encoders, List.of(createPatientWithMaritalStatusCoding()));
      final Dataset<Row> codingDf = ds.read("Patient");

      final SingleInstanceEvaluationResult result =
          SingleInstanceEvaluator.evaluate(
              codingDf,
              "Patient",
              fhirContext,
              "Patient.maritalStatus.coding.first().trace('t')"
                  + " = Patient.maritalStatus.coding.first()",
              null,
              null);

      final long count =
          result.getTraces().stream()
              .filter(t -> "t".equals(t.getLabel()))
              .mapToLong(t -> t.getValues().size())
              .sum();

      assertEquals(
          1,
          count,
          "Trace in Coding equality (via CodingEquality.equalsTo) should fire exactly once."
              + " See issue #2594.");
    }
  }

  /**
   * Parameters for a single trace-entry-count scenario.
   *
   * @param expression the FHIRPath expression to evaluate
   * @param label the trace label to count entries for
   * @param expected the expected total number of trace entry values for {@code label}
   */
  record TraceEntryCase(String expression, String label, int expected) {
    @Override
    public String toString() {
      return expression;
    }
  }

  private static Patient createPatientWithThreeNames() {
    // Fixture from issue #2594 — do not alter without updating the issue reference.
    final Patient p = new Patient();
    p.setId("Patient/three-names");
    p.addName()
        .setUse(HumanName.NameUse.OFFICIAL)
        .setFamily("Smith")
        .addGiven("John")
        .addGiven("Quincy");
    p.addName().setUse(HumanName.NameUse.USUAL).setFamily("Smith").addGiven("Johnny");
    p.addName().setUse(HumanName.NameUse.MAIDEN).setFamily("Doe").addGiven("John").addGiven("Q");
    return p;
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

  private static Patient createPatientWithMaritalStatusCoding() {
    final Patient p = new Patient();
    p.setId("Patient/with-coding");
    final CodeableConcept maritalStatus = new CodeableConcept();
    maritalStatus.addCoding(
        new Coding("http://terminology.hl7.org/CodeSystem/v3-MaritalStatus", "M", "Married"));
    p.setMaritalStatus(maritalStatus);
    return p;
  }
}
