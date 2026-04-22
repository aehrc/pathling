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

package au.csiro.pathling.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import scala.collection.immutable.IndexedSeq;
import scala.jdk.javaapi.CollectionConverters;

/**
 * Tests for {@link TraceExpression} and {@link TraceProjectionExpression}, including the shared
 * logging and collection logic in the companion {@code TraceHelper} object.
 *
 * @author John Grimes
 */
class TraceExpressionTest {

  /**
   * Captures trace entries reported via the {@link TraceCollector} interface so that collector
   * interactions can be asserted from tests.
   */
  private static final class CapturingCollector implements TraceCollector {

    /** Immutable record of a single {@code add} invocation. */
    private record Entry(@Nonnull String label, @Nonnull String fhirType, @Nullable Object value) {}

    private final List<Entry> entries = new ArrayList<>();

    @Override
    public void add(
        @Nonnull final String label, @Nonnull final String fhirType, @Nullable final Object value) {
      entries.add(new Entry(label, fhirType, value));
    }
  }

  private Logger traceLogger;
  private Level originalLevel;
  private ListAppender<ILoggingEvent> appender;

  @BeforeEach
  void setUp() {
    // Attach a capturing appender so that log output from the TraceExpression logger can be
    // asserted by tests. The logger level is restored in tearDown to avoid polluting other tests.
    traceLogger = (Logger) LoggerFactory.getLogger(TraceExpression.class);
    originalLevel = traceLogger.getLevel();
    appender = new ListAppender<>();
    appender.start();
    traceLogger.addAppender(appender);
  }

  @AfterEach
  void tearDown() {
    traceLogger.detachAppender(appender);
    traceLogger.setLevel(originalLevel);
    appender.stop();
  }

  // ------------------------------------------------------------------
  // Helpers.
  // ------------------------------------------------------------------

  @Nonnull
  private static Expression stringLiteral(@Nullable final String value) {
    return new Literal(value == null ? null : UTF8String.fromString(value), DataTypes.StringType);
  }

  @Nonnull
  private static Expression integerLiteral(@Nullable final Integer value) {
    return new Literal(value, DataTypes.IntegerType);
  }

  @Nonnull
  private static Expression arrayLiteral(
      @Nonnull final Object[] catalystValues, @Nonnull final DataType elementType) {
    final ArrayData arrayData = new GenericArrayData(catalystValues);
    return new Literal(arrayData, DataTypes.createArrayType(elementType));
  }

  @Nonnull
  private static Expression structLiteral(
      @Nonnull final Object[] catalystValues, @Nonnull final StructType schema) {
    final InternalRow row = new GenericInternalRow(catalystValues);
    return new Literal(row, schema);
  }

  /**
   * Evaluates a {@link TraceExpression} after initialising it (required for any {@code
   * Nondeterministic} expression).
   */
  @Nullable
  private static Object evalTrace(@Nonnull final TraceExpression expr) {
    expr.initialize(0);
    return expr.eval(InternalRow.empty());
  }

  /** Evaluates a {@link TraceProjectionExpression} after initialising it. */
  @Nullable
  private static Object evalTraceProjection(@Nonnull final TraceProjectionExpression expr) {
    expr.initialize(0);
    return expr.eval(InternalRow.empty());
  }

  // ------------------------------------------------------------------
  // TraceExpression behaviour.
  // ------------------------------------------------------------------

  @Test
  void traceReturnsChildValueUnchangedAndInvokesCollector() {
    // When the child yields a non-null value and a collector is supplied, the value is passed to
    // the collector (converted to its external Scala representation) and also returned unchanged.
    final CapturingCollector collector = new CapturingCollector();
    final TraceExpression expr =
        new TraceExpression(stringLiteral("hello"), "label", "string", collector);

    final Object result = evalTrace(expr);

    assertEquals(UTF8String.fromString("hello"), result);
    assertEquals(1, collector.entries.size());
    assertEquals("label", collector.entries.get(0).label());
    assertEquals("string", collector.entries.get(0).fhirType());
    // The collector receives the converted Scala value, i.e. a Java String (not UTF8String).
    assertEquals("hello", collector.entries.get(0).value());
  }

  @Test
  void traceReturnsNullWithoutInvokingCollectorWhenChildIsNull() {
    // A null child value must short-circuit so that the collector is never called and no log
    // output is produced.
    traceLogger.setLevel(Level.TRACE);
    final CapturingCollector collector = new CapturingCollector();
    final TraceExpression expr =
        new TraceExpression(stringLiteral(null), "label", "string", collector);

    final Object result = evalTrace(expr);

    assertNull(result);
    assertTrue(collector.entries.isEmpty());
    assertTrue(appender.list.isEmpty());
  }

  @Test
  void traceLogsWhenTraceEnabledAndNoCollector() {
    // With trace logging enabled and no collector, the value should still be logged and returned.
    traceLogger.setLevel(Level.TRACE);
    final TraceExpression expr = new TraceExpression(stringLiteral("v"), "label", "string", null);

    final Object result = evalTrace(expr);

    assertEquals(UTF8String.fromString("v"), result);
    assertEquals(1, appender.list.size());
    assertTrue(appender.list.get(0).getFormattedMessage().contains("[trace:label]"));
  }

  @Test
  void traceDoesNotLogWhenTraceDisabledAndNoCollector() {
    // With trace logging disabled and no collector, neither logging nor conversion should occur,
    // but the value must still be returned unchanged.
    traceLogger.setLevel(Level.INFO);
    final TraceExpression expr = new TraceExpression(stringLiteral("v"), "label", "string", null);

    final Object result = evalTrace(expr);

    assertEquals(UTF8String.fromString("v"), result);
    assertTrue(appender.list.isEmpty());
  }

  @Test
  void traceDataTypeAndNullableDelegateToChild() {
    final CapturingCollector collector = new CapturingCollector();
    final TraceExpression expr =
        new TraceExpression(stringLiteral("v"), "label", "string", collector);

    assertEquals(DataTypes.StringType, expr.dataType());
    assertFalse(expr.nullable());
  }

  @Test
  void tracePrettyNameIsTrace() {
    final TraceExpression expr = new TraceExpression(stringLiteral("v"), "label", "string", null);
    assertEquals("trace", expr.prettyName());
  }

  @Test
  void traceWithNewChildInternalReturnsCopyWithReplacedChild() {
    final CapturingCollector collector = new CapturingCollector();
    final Expression originalChild = stringLiteral("old");
    final Expression replacement = stringLiteral("new");
    final TraceExpression expr = new TraceExpression(originalChild, "label", "string", collector);

    final IndexedSeq<Expression> newChildren =
        CollectionConverters.asScala(java.util.List.of(replacement)).toIndexedSeq();
    final Expression updated = (Expression) expr.withNewChildrenInternal(newChildren);

    assertInstanceOf(TraceExpression.class, updated);
    assertNotSame(expr, updated);
    final TraceExpression updatedTrace = (TraceExpression) updated;
    assertEquals(replacement, updatedTrace.child());
    assertEquals("label", updatedTrace.name());
    assertEquals("string", updatedTrace.fhirType());
    assertEquals(collector, updatedTrace.collector());
  }

  // ------------------------------------------------------------------
  // TraceProjectionExpression behaviour.
  // ------------------------------------------------------------------

  @Test
  void traceProjectionReturnsLeftAndLogsRightWhenBothNonNull() {
    // The projection form returns the pass-through (left) value while logging the projected
    // (right) value.
    traceLogger.setLevel(Level.TRACE);
    final CapturingCollector collector = new CapturingCollector();
    final TraceProjectionExpression expr =
        new TraceProjectionExpression(
            stringLiteral("pass"), stringLiteral("log"), "label", "string", collector);

    final Object result = evalTraceProjection(expr);

    assertEquals(UTF8String.fromString("pass"), result);
    assertEquals(1, collector.entries.size());
    assertEquals("log", collector.entries.get(0).value());
    assertEquals(1, appender.list.size());
  }

  @Test
  void traceProjectionReturnsNullWithoutLoggingWhenLeftIsNull() {
    // When the pass-through value is null, the projection must short-circuit: no logging, no
    // collection, and the right expression is not evaluated.
    traceLogger.setLevel(Level.TRACE);
    final CapturingCollector collector = new CapturingCollector();
    final TraceProjectionExpression expr =
        new TraceProjectionExpression(
            stringLiteral(null), stringLiteral("log"), "label", "string", collector);

    final Object result = evalTraceProjection(expr);

    assertNull(result);
    assertTrue(collector.entries.isEmpty());
    assertTrue(appender.list.isEmpty());
  }

  @Test
  void traceProjectionReturnsLeftWithoutLoggingWhenRightIsNull() {
    // A null projected value skips logging but still returns the pass-through value.
    traceLogger.setLevel(Level.TRACE);
    final CapturingCollector collector = new CapturingCollector();
    final TraceProjectionExpression expr =
        new TraceProjectionExpression(
            stringLiteral("pass"), stringLiteral(null), "label", "string", collector);

    final Object result = evalTraceProjection(expr);

    assertEquals(UTF8String.fromString("pass"), result);
    assertTrue(collector.entries.isEmpty());
    assertTrue(appender.list.isEmpty());
  }

  @Test
  void traceProjectionDataTypeAndNullableDelegateToLeft() {
    final TraceProjectionExpression expr =
        new TraceProjectionExpression(
            stringLiteral("pass"), integerLiteral(1), "label", "integer", null);

    // The expression exposes the left (pass-through) schema, not the right.
    assertEquals(DataTypes.StringType, expr.dataType());
    assertFalse(expr.nullable());
  }

  @Test
  void traceProjectionPrettyNameIsTrace() {
    final TraceProjectionExpression expr =
        new TraceProjectionExpression(
            stringLiteral("pass"), stringLiteral("log"), "label", "string", null);
    assertEquals("trace", expr.prettyName());
  }

  @Test
  void traceProjectionWithNewChildrenInternalReturnsCopyWithReplacedChildren() {
    final CapturingCollector collector = new CapturingCollector();
    final Expression newLeft = stringLiteral("newLeft");
    final Expression newRight = stringLiteral("newRight");
    final TraceProjectionExpression expr =
        new TraceProjectionExpression(
            stringLiteral("pass"), stringLiteral("log"), "label", "string", collector);

    final IndexedSeq<Expression> newChildren =
        CollectionConverters.asScala(java.util.List.of(newLeft, newRight)).toIndexedSeq();
    final Expression updated = (Expression) expr.withNewChildrenInternal(newChildren);

    assertInstanceOf(TraceProjectionExpression.class, updated);
    assertNotSame(expr, updated);
    final TraceProjectionExpression updatedProjection = (TraceProjectionExpression) updated;
    assertEquals(newLeft, updatedProjection.left());
    assertEquals(newRight, updatedProjection.right());
    assertEquals("label", updatedProjection.name());
    assertEquals("string", updatedProjection.fhirType());
    assertEquals(collector, updatedProjection.collector());
  }

  @Test
  void traceProjectionDoesNotLogWhenTraceDisabledAndNoCollector() {
    // Skipping logging and collection is an important optimisation when nothing will consume the
    // traced value; verify that the right-hand expression result is still discarded.
    traceLogger.setLevel(Level.INFO);
    final TraceProjectionExpression expr =
        new TraceProjectionExpression(
            stringLiteral("pass"), stringLiteral("log"), "label", "string", null);

    final Object result = evalTraceProjection(expr);

    assertEquals(UTF8String.fromString("pass"), result);
    assertTrue(appender.list.isEmpty());
  }

  // ------------------------------------------------------------------
  // TraceHelper.toReadableString branches, exercised via log output.
  // ------------------------------------------------------------------

  @Test
  void logFormatsStringWithEscapedSpecialCharacters() {
    // Backslashes are escaped first, then quotes, to avoid double-escaping. The expected output
    // therefore treats the original backslash and quote independently.
    traceLogger.setLevel(Level.TRACE);
    final TraceExpression expr =
        new TraceExpression(stringLiteral("a\\b\"c"), "label", "string", null);

    evalTrace(expr);

    assertEquals(1, appender.list.size());
    assertTrue(
        appender.list.get(0).getFormattedMessage().contains("\"a\\\\b\\\"c\""),
        "Expected escaped string in log output: " + appender.list.get(0).getFormattedMessage());
  }

  @Test
  void logFormatsIntegerValueViaStringValueOfFallback() {
    // The Integer path exercises the catch-all branch of toReadableString.
    traceLogger.setLevel(Level.TRACE);
    final TraceExpression expr = new TraceExpression(integerLiteral(42), "label", "integer", null);

    evalTrace(expr);

    assertEquals(1, appender.list.size());
    final String message = appender.list.get(0).getFormattedMessage();
    assertTrue(message.contains("42"), "Expected integer rendered in log: " + message);
  }

  @Test
  void logFormatsArrayWithNullElementsRecursively() {
    // An array value exercises the Seq branch of toReadableString. Including a null element and
    // a quoted string also exercises both the null branch (reached recursively) and the String
    // escaping branch in a single evaluation.
    traceLogger.setLevel(Level.TRACE);
    final Expression array =
        arrayLiteral(
            new Object[] {UTF8String.fromString("a"), null, UTF8String.fromString("b")},
            DataTypes.StringType);
    final TraceExpression expr = new TraceExpression(array, "label", "string", null);

    evalTrace(expr);

    assertEquals(1, appender.list.size());
    final String message = appender.list.get(0).getFormattedMessage();
    assertTrue(
        message.contains("[\"a\", null, \"b\"]"), "Expected array rendered in log: " + message);
  }

  @Test
  void logFormatsStructValueUsingRowJson() {
    // A struct value exercises the Row branch of toReadableString, which calls Row.json.
    traceLogger.setLevel(Level.TRACE);
    final StructType schema =
        DataTypes.createStructType(
            new StructField[] {DataTypes.createStructField("f", DataTypes.StringType, true)});
    final Expression struct = structLiteral(new Object[] {UTF8String.fromString("v")}, schema);
    final TraceExpression expr = new TraceExpression(struct, "label", "HumanName", null);

    evalTrace(expr);

    assertEquals(1, appender.list.size());
    final String message = appender.list.get(0).getFormattedMessage();
    assertTrue(message.contains("\"f\""), "Expected JSON-rendered struct in log: " + message);
    assertTrue(message.contains("\"v\""), "Expected JSON-rendered struct value in log: " + message);
  }

  // ------------------------------------------------------------------
  // Case class synthetic members (equals, hashCode, copy, product*, toString).
  // Exercising these branches is necessary for SonarCloud branch coverage on the new code.
  // ------------------------------------------------------------------

  @Test
  void traceCaseClassEqualityAndHashCodeReflectAllFields() {
    // Two instances built from equal field values must be equal and share a hash code, while any
    // field difference must make them unequal. This exercises the generated equals/hashCode
    // branches for each case class field.
    final CapturingCollector collector = new CapturingCollector();
    final TraceExpression base =
        new TraceExpression(stringLiteral("v"), "label", "string", collector);
    final TraceExpression same =
        new TraceExpression(stringLiteral("v"), "label", "string", collector);
    final TraceExpression differentChild =
        new TraceExpression(stringLiteral("other"), "label", "string", collector);
    final TraceExpression differentName =
        new TraceExpression(stringLiteral("v"), "other", "string", collector);
    final TraceExpression differentType =
        new TraceExpression(stringLiteral("v"), "label", "Quantity", collector);
    final TraceExpression differentCollector =
        new TraceExpression(stringLiteral("v"), "label", "string", new CapturingCollector());

    assertEquals(base, base);
    assertEquals(base, same);
    assertEquals(base.hashCode(), same.hashCode());
    assertFalse(base.equals(differentChild));
    assertFalse(base.equals(differentName));
    assertFalse(base.equals(differentType));
    assertFalse(base.equals(differentCollector));
    assertFalse(base.equals(null));
    assertFalse(base.equals("not a trace expression"));
  }

  @Test
  void traceCaseClassProductMembersExposeFields() {
    // The Product contract exposes the case class fields by index. Verifying it touches the
    // generated productArity/productElement branches.
    final CapturingCollector collector = new CapturingCollector();
    final Expression child = stringLiteral("v");
    final TraceExpression expr = new TraceExpression(child, "label", "string", collector);

    assertEquals(4, expr.productArity());
    assertEquals(child, expr.productElement(0));
    assertEquals("label", expr.productElement(1));
    assertEquals("string", expr.productElement(2));
    assertEquals(collector, expr.productElement(3));
    assertTrue(expr.toString().contains("label"));
  }

  @Test
  void traceCaseClassCopyReplacesSelectedField() {
    // The Scala-generated copy method produces a new instance with selected fields replaced. We
    // use it to cover the default-argument branches for each case class field.
    final CapturingCollector collector = new CapturingCollector();
    final Expression child = stringLiteral("v");
    final TraceExpression original = new TraceExpression(child, "label", "string", collector);

    final TraceExpression copy =
        original.copy(child, original.name(), original.fhirType(), original.collector());

    assertEquals(original, copy);
    assertNotSame(original, copy);
  }

  @Test
  void traceProjectionCaseClassEqualityAndHashCodeReflectAllFields() {
    final CapturingCollector collector = new CapturingCollector();
    final TraceProjectionExpression base =
        new TraceProjectionExpression(
            stringLiteral("p"), stringLiteral("l"), "label", "string", collector);
    final TraceProjectionExpression same =
        new TraceProjectionExpression(
            stringLiteral("p"), stringLiteral("l"), "label", "string", collector);
    final TraceProjectionExpression differentLeft =
        new TraceProjectionExpression(
            stringLiteral("other"), stringLiteral("l"), "label", "string", collector);
    final TraceProjectionExpression differentRight =
        new TraceProjectionExpression(
            stringLiteral("p"), stringLiteral("other"), "label", "string", collector);
    final TraceProjectionExpression differentName =
        new TraceProjectionExpression(
            stringLiteral("p"), stringLiteral("l"), "other", "string", collector);
    final TraceProjectionExpression differentType =
        new TraceProjectionExpression(
            stringLiteral("p"), stringLiteral("l"), "label", "Quantity", collector);
    final TraceProjectionExpression differentCollector =
        new TraceProjectionExpression(
            stringLiteral("p"), stringLiteral("l"), "label", "string", new CapturingCollector());

    assertEquals(base, base);
    assertEquals(base, same);
    assertEquals(base.hashCode(), same.hashCode());
    assertFalse(base.equals(differentLeft));
    assertFalse(base.equals(differentRight));
    assertFalse(base.equals(differentName));
    assertFalse(base.equals(differentType));
    assertFalse(base.equals(differentCollector));
    assertFalse(base.equals(null));
    assertFalse(base.equals("not a trace expression"));
  }

  @Test
  void traceProjectionCaseClassProductMembersExposeFields() {
    final CapturingCollector collector = new CapturingCollector();
    final Expression left = stringLiteral("p");
    final Expression right = stringLiteral("l");
    final TraceProjectionExpression expr =
        new TraceProjectionExpression(left, right, "label", "string", collector);

    assertEquals(5, expr.productArity());
    assertEquals(left, expr.productElement(0));
    assertEquals(right, expr.productElement(1));
    assertEquals("label", expr.productElement(2));
    assertEquals("string", expr.productElement(3));
    assertEquals(collector, expr.productElement(4));
    assertTrue(expr.toString().contains("label"));
  }

  @Test
  void traceProjectionCaseClassCopyReplacesSelectedField() {
    final CapturingCollector collector = new CapturingCollector();
    final Expression left = stringLiteral("p");
    final Expression right = stringLiteral("l");
    final TraceProjectionExpression original =
        new TraceProjectionExpression(left, right, "label", "string", collector);

    final TraceProjectionExpression copy =
        original.copy(left, right, original.name(), original.fhirType(), original.collector());

    assertEquals(original, copy);
    assertNotSame(original, copy);
  }

  // ------------------------------------------------------------------
  // Companion object and trait defaults.
  // ------------------------------------------------------------------

  @Test
  void traceNondeterministicAndFoldableExposeExpectedFlags() {
    // The Nondeterministic trait fixes deterministic=false and foldable=false. Invoking these
    // forces the generated lazy-val and overridden accessor to execute.
    final TraceExpression expr = new TraceExpression(stringLiteral("v"), "label", "string", null);
    final TraceProjectionExpression projection =
        new TraceProjectionExpression(
            stringLiteral("p"), stringLiteral("l"), "label", "string", null);

    assertFalse(expr.deterministic());
    assertFalse(expr.foldable());
    assertFalse(projection.deterministic());
    assertFalse(projection.foldable());
  }

  @Test
  void traceCompanionApplyAndUnapplyRoundTripFields() {
    // Scala call sites use the companion object's apply/unapply instead of the constructor. We
    // invoke them via MODULE$ to exercise the synthesised factory methods.
    final CapturingCollector collector = new CapturingCollector();
    final Expression child = stringLiteral("v");
    final TraceExpression constructed =
        TraceExpression$.MODULE$.apply(child, "label", "string", collector);
    final TraceProjectionExpression projection =
        TraceProjectionExpression$.MODULE$.apply(
            stringLiteral("p"), stringLiteral("l"), "label", "string", collector);

    assertEquals(child, constructed.child());
    assertEquals("label", constructed.name());
    assertEquals("string", constructed.fhirType());
    assertEquals(collector, constructed.collector());
    assertTrue(TraceExpression$.MODULE$.unapply(constructed).isDefined());

    assertEquals("label", projection.name());
    assertTrue(TraceProjectionExpression$.MODULE$.unapply(projection).isDefined());
  }

  @Test
  void traceProductElementNameExposesFieldNames() {
    // Scala 2.13 case classes expose field names via productElementName. This branch is commonly
    // missed when a case class is only constructed from Java.
    final TraceExpression expr = new TraceExpression(stringLiteral("v"), "label", "string", null);
    final TraceProjectionExpression projection =
        new TraceProjectionExpression(
            stringLiteral("p"), stringLiteral("l"), "label", "string", null);

    assertEquals("child", expr.productElementName(0));
    assertEquals("name", expr.productElementName(1));
    assertEquals("fhirType", expr.productElementName(2));
    assertEquals("collector", expr.productElementName(3));

    assertEquals("left", projection.productElementName(0));
    assertEquals("right", projection.productElementName(1));
    assertEquals("name", projection.productElementName(2));
    assertEquals("fhirType", projection.productElementName(3));
    assertEquals("collector", projection.productElementName(4));
  }
}
