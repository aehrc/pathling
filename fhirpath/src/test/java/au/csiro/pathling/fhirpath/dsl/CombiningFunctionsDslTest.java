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

package au.csiro.pathling.fhirpath.dsl;

import static au.csiro.pathling.test.yaml.FhirTypedLiteral.toCoding;
import static au.csiro.pathling.test.yaml.FhirTypedLiteral.toDate;
import static au.csiro.pathling.test.yaml.FhirTypedLiteral.toDateTime;
import static au.csiro.pathling.test.yaml.FhirTypedLiteral.toQuantity;
import static au.csiro.pathling.test.yaml.FhirTypedLiteral.toTime;

import au.csiro.pathling.test.dsl.FhirPathDslTestBase;
import au.csiro.pathling.test.dsl.FhirPathTest;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;

/**
 * Tests for the FHIRPath combining functions {@code union()} and {@code combine()}. The function
 * forms are parser-desugared into the same AST used by the {@code |} operator (for {@code union})
 * and a peer {@code CombineOperator} (for {@code combine}), so this class focuses on proving the
 * desugaring works across the type matrix, the iteration-context equivalence with the operator
 * form, the duplicate-preservation semantics of {@code combine}, and the arity / type error paths.
 * Exhaustive merge-semantics coverage for the {@code union} case is provided by {@link
 * CombiningOperatorsDslTest}.
 */
public class CombiningFunctionsDslTest extends FhirPathDslTestBase {

  // ---------------------------------------------------------------------------------------------
  // union() — parser desugaring sanity across the type matrix.
  // ---------------------------------------------------------------------------------------------

  @FhirPathTest
  public Stream<DynamicTest> testUnionFunctionEmptyCollections() {
    return builder()
        .withSubject(sb -> sb)
        .group("union() empty operand handling")
        .testEmpty("{}.union({})", "union() of two empty collections is empty")
        .testEmpty("({} | {}).union({} | {})", "Grouped empty unions via function form")
        .testEquals(
            List.of(1, 2),
            "{}.union(1 | 1 | 2)",
            "union() with empty input deduplicates the argument collection")
        .testEquals(
            List.of(1, 2, 3),
            "(1 | 1 | 2 | 3).union({})",
            "union() with empty argument deduplicates the input collection")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testUnionFunctionPrimitives() {
    return builder()
        .withSubject(
            sb ->
                sb.boolArray("trueFalse", true, false)
                    .integerArray("oneTwo", 1, 2)
                    .integerArray("twoThree", 2, 3)
                    .stringArray("abc", "a", "b", "c")
                    .stringArray("bcd", "b", "c", "d")
                    .decimalArray("dec12", 1.1, 2.2))
        .group("union() smoke tests for primitive types")
        .testEquals(List.of(true, false), "true.union(false)", "Boolean union()")
        .testTrue("true.union(true)", "Boolean union() deduplicates")
        .testEquals(
            List.of(1, 2, 3), "oneTwo.union(twoThree)", "Integer union() merges and deduplicates")
        .testEquals(
            List.of(1, 2), "oneTwo.union(oneTwo)", "Integer union() deduplicates identical arrays")
        .testEquals(
            List.of("a", "b", "c", "d"), "abc.union(bcd)", "String union() merges and deduplicates")
        .testEquals(
            List.of(1.1, 2.2), "dec12.union(dec12)", "Decimal union() deduplicates identical")
        .testEquals(List.of(1.0, 2.0), "1.union(2.0)", "Integer.union(Decimal) promotes to Decimal")
        .testEquals(List.of(1.0, 2.0), "1.0.union(2)", "Decimal.union(Integer) promotes to Decimal")
        .testEquals(List.of(true, false), "trueFalse.union({})", "union({}) deduplicates input")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testUnionFunctionTemporalAndQuantityAndCoding() {
    return builder()
        .withSubject(sb -> sb)
        .group("union() smoke tests for temporal, Quantity, and Coding types")
        .testEquals(
            toDate("2020-01-01"),
            "@2020-01-01.union(@2020-01-01)",
            "Date union() deduplicates identical dates")
        .testEquals(
            List.of(toDate("2020-01-01"), toDate("2020-01-02")),
            "@2020-01-01.union(@2020-01-02)",
            "Date union() merges distinct dates")
        .testEquals(
            toDateTime("2020-01-01T10:00:00"),
            "@2020-01-01T10:00:00.union(@2020-01-01T10:00:00)",
            "DateTime union() deduplicates identical datetimes")
        .testEquals(
            toTime("12:00"), "@T12:00.union(@T12:00)", "Time union() deduplicates identical times")
        .testEquals(
            toQuantity("1000 'mg'"),
            "1000 'mg'.union(1 'g')",
            "Quantity union() recognises equal values in different units")
        .testEquals(
            toCoding("http://loinc.org|8867-4||'Heart rate'"),
            "http://loinc.org|8867-4||'Heart rate'.union("
                + "http://loinc.org|8867-4||'Heart rate')",
            "Coding union() deduplicates identical codings")
        .build();
  }

  // ---------------------------------------------------------------------------------------------
  // combine() — full coverage (this is the genuinely new behaviour).
  // ---------------------------------------------------------------------------------------------

  @FhirPathTest
  public Stream<DynamicTest> testCombineEmptyCollections() {
    return builder()
        .withSubject(sb -> sb)
        .group("combine() empty operand handling")
        .testEmpty("{}.combine({})", "combine() of two empty collections is empty")
        .testEquals(1, "{}.combine(1)", "combine() with empty input returns non-empty argument")
        .testEquals(1, "1.combine({})", "combine() with empty argument returns non-empty input")
        .testEquals(
            List.of(1, 2, 3),
            "{}.combine(1 | 2 | 3)",
            "combine({}, non-empty) returns the non-empty operand")
        .testEquals(
            List.of(1, 2, 3),
            "(1 | 2 | 3).combine({})",
            "combine(non-empty, {}) returns the non-empty operand")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testCombinePreservesDuplicates() {
    return builder()
        .withSubject(
            sb ->
                sb.integerArray("oneOne", 1, 1)
                    .integerArray("oneTwoThree", 1, 2, 3)
                    .stringArray("aa", "a", "a")
                    .stringArray("ab", "a", "b"))
        .group("combine() duplicate preservation")
        .testEquals(
            List.of(1, 1),
            "1.combine(1)",
            "combine() of two equal singletons preserves both copies")
        .testEquals(
            List.of(1, 1, 1),
            "oneOne.combine(1)",
            "combine() preserves duplicates within left operand")
        .testEquals(
            List.of(1, 1, 1, 2, 3),
            "oneOne.combine(oneTwoThree)",
            "combine() concatenates without deduplicating between operands")
        .testEquals(
            List.of(1, 2, 3, 1, 1),
            "oneTwoThree.combine(oneOne)",
            "combine() preserves operand order of duplicates (right concatenation)")
        .testEquals(
            List.of("a", "a", "a", "b"),
            "aa.combine(ab)",
            "combine() preserves duplicates across String arrays")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testCombineTypePromotion() {
    return builder()
        .withSubject(sb -> sb)
        .group("combine() type reconciliation")
        .testEquals(List.of(1.0, 2.0), "1.combine(2.0)", "combine() promotes Integer to Decimal")
        .testEquals(
            List.of(1.0, 2.0, 2.0),
            "(1 | 2).combine(2.0)",
            "combine() promotes Integer collection to Decimal and preserves duplicates")
        .testEquals(
            List.of(1.0, 1.0),
            "1.combine(1.0)",
            "combine() does not deduplicate equal values across promoted types")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testCombineQuantityAndCodingNoDedup() {
    return builder()
        .withSubject(sb -> sb)
        .group("combine() does not deduplicate Quantity or Coding values")
        .testEquals(
            List.of(toQuantity("1000 'mg'"), toQuantity("1 'g'")),
            "1000 'mg'.combine(1 'g')",
            "combine() keeps both Quantity values even when they are equal under Quantity"
                + " equality")
        .testEquals(
            List.of(
                toCoding("http://loinc.org|8867-4||'Heart rate'"),
                toCoding("http://loinc.org|8867-4||'Heart rate'")),
            "http://loinc.org|8867-4||'Heart rate'.combine("
                + "http://loinc.org|8867-4||'Heart rate')",
            "combine() keeps both Coding values when they are identical")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testCombineTemporal() {
    return builder()
        .withSubject(sb -> sb)
        .group("combine() on temporal types does not deduplicate")
        .testEquals(
            List.of(toDate("2020-01-01"), toDate("2020-01-01")),
            "@2020-01-01.combine(@2020-01-01)",
            "combine() preserves identical Date values")
        .testEquals(
            List.of(toDateTime("2020-01-01T10:00:00"), toDateTime("2020-01-01T10:00:00")),
            "@2020-01-01T10:00:00.combine(@2020-01-01T10:00:00)",
            "combine() preserves identical DateTime values")
        .testEquals(
            List.of(toTime("12:00"), toTime("12:00")),
            "@T12:00.combine(@T12:00)",
            "combine() preserves identical Time values")
        .build();
  }

  // ---------------------------------------------------------------------------------------------
  // union() is equivalent to the `|` operator across the type matrix.
  // ---------------------------------------------------------------------------------------------

  @FhirPathTest
  public Stream<DynamicTest> testUnionFunctionEquivalentToPipe() {
    return builder()
        .withSubject(sb -> sb.integerArray("ints", 1, 2, 3).stringArray("strs", "a", "b"))
        .group("union() and `|` produce identical results")
        .testEquals(List.of(1, 2, 3), "ints.union(ints)", "union() self-merge deduplicates")
        .testEquals(List.of(1, 2, 3), "ints | ints", "`|` self-merge deduplicates (control)")
        .testEquals(List.of("a", "b"), "strs.union(strs)", "union() self-merge deduplicates")
        .testEquals(List.of("a", "b"), "strs | strs", "`|` self-merge deduplicates (control)")
        .testEquals(
            List.of(1, 2, 3, 4), "ints.union(4)", "union() with literal argument merges with dedup")
        .testEquals(
            List.of(1, 2, 3, 4),
            "ints | 4",
            "`|` with literal argument merges with dedup (control)")
        .testEquals(
            List.of(1, 2, 3, 4),
            "(ints.union(3)).union(4)",
            "Chained union() calls behave like chained `|`")
        .testEquals(
            List.of(1, 2, 3, 4),
            "(ints | 3) | 4",
            "Chained `|` operators produce the same result (control)")
        .build();
  }

  /**
   * Pairs a {@code union()} call against the equivalent {@code |} expression across the full
   * FHIRPath type matrix. Every primitive type required by the spec's "Union is equivalent to the
   * pipe operator" scenario gets a direct per-type comparison here, so that any future regression
   * that breaks the parser desugaring (or the structural equivalence it relies on) fails loudly in
   * this test.
   */
  @FhirPathTest
  public Stream<DynamicTest> testUnionFunctionEquivalentToPipeAcrossTypeMatrix() {
    return builder()
        .withSubject(sb -> sb)
        .group("union() ≡ `|` — Boolean")
        .testEquals(List.of(true, false), "true.union(false)", "Boolean union() function form")
        .testEquals(List.of(true, false), "true | false", "Boolean union operator form (control)")
        .group("union() ≡ `|` — Integer")
        .testEquals(List.of(1, 2, 3), "(1 | 2).union(2 | 3)", "Integer union() function form")
        .testEquals(List.of(1, 2, 3), "(1 | 2) | (2 | 3)", "Integer union operator form (control)")
        .group("union() ≡ `|` — Decimal")
        .testEquals(
            List.of(1.1, 2.2, 3.3), "(1.1 | 2.2).union(2.2 | 3.3)", "Decimal union() function form")
        .testEquals(
            List.of(1.1, 2.2, 3.3),
            "(1.1 | 2.2) | (2.2 | 3.3)",
            "Decimal union operator form (control)")
        .group("union() ≡ `|` — String")
        .testEquals(
            List.of("a", "b", "c"), "('a' | 'b').union('b' | 'c')", "String union() function form")
        .testEquals(
            List.of("a", "b", "c"),
            "('a' | 'b') | ('b' | 'c')",
            "String union operator form (control)")
        .group("union() ≡ `|` — Date")
        .testEquals(
            List.of(toDate("2020-01-01"), toDate("2020-01-02")),
            "@2020-01-01.union(@2020-01-02)",
            "Date union() function form")
        .testEquals(
            List.of(toDate("2020-01-01"), toDate("2020-01-02")),
            "@2020-01-01 | @2020-01-02",
            "Date union operator form (control)")
        .group("union() ≡ `|` — DateTime")
        .testEquals(
            List.of(toDateTime("2020-01-01T10:00:00"), toDateTime("2020-01-01T11:00:00")),
            "@2020-01-01T10:00:00.union(@2020-01-01T11:00:00)",
            "DateTime union() function form")
        .testEquals(
            List.of(toDateTime("2020-01-01T10:00:00"), toDateTime("2020-01-01T11:00:00")),
            "@2020-01-01T10:00:00 | @2020-01-01T11:00:00",
            "DateTime union operator form (control)")
        .group("union() ≡ `|` — Time")
        .testEquals(
            List.of(toTime("12:00"), toTime("13:00")),
            "@T12:00.union(@T13:00)",
            "Time union() function form")
        .testEquals(
            List.of(toTime("12:00"), toTime("13:00")),
            "@T12:00 | @T13:00",
            "Time union operator form (control)")
        .group("union() ≡ `|` — Quantity")
        .testEquals(
            toQuantity("1000 'mg'"),
            "1000 'mg'.union(1 'g')",
            "Quantity union() function form (equal values, first retained)")
        .testEquals(
            toQuantity("1000 'mg'"), "1000 'mg' | 1 'g'", "Quantity union operator form (control)")
        .group("union() ≡ `|` — Coding")
        .testEquals(
            List.of(
                toCoding("http://loinc.org|8867-4||'Heart rate'"),
                toCoding("http://loinc.org|8480-6||'Systolic blood pressure'")),
            "http://loinc.org|8867-4||'Heart rate'.union("
                + "http://loinc.org|8480-6||'Systolic blood pressure')",
            "Coding union() function form")
        .testEquals(
            List.of(
                toCoding("http://loinc.org|8867-4||'Heart rate'"),
                toCoding("http://loinc.org|8480-6||'Systolic blood pressure'")),
            "http://loinc.org|8867-4||'Heart rate' | "
                + "http://loinc.org|8480-6||'Systolic blood pressure'",
            "Coding union operator form (control)")
        .build();
  }

  // ---------------------------------------------------------------------------------------------
  // Iteration-context pin-down tests — the critical reason for parser desugaring.
  // These expressions would silently return wrong results if union()/combine() were implemented
  // as regular method-defined functions whose arguments go through the standard
  // FunctionParameterResolver path.
  // ---------------------------------------------------------------------------------------------

  @FhirPathTest
  public Stream<DynamicTest> testUnionIterationContextInsideSelect() {
    return builder()
        .withSubject(
            sb ->
                sb.elementArray(
                    "names",
                    n1 -> n1.string("use", "official").stringArray("given", "John", "Johnny"),
                    n2 -> n2.string("use", "maiden").stringArray("given", "Smith")))
        .group(
            "union() honours the surrounding iteration context"
                + " (name.select(use.union(given)) ≡ name.select(use | given))")
        .testEquals(
            List.of("official", "John", "Johnny", "maiden", "Smith"),
            "names.select(use.union(given))",
            "Function form: `given` resolves against the current name element, not the root")
        .testEquals(
            List.of("official", "John", "Johnny", "maiden", "Smith"),
            "names.select(use | given)",
            "Operator form produces the same result (control)")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testUnionIterationContextWithOverlap() {
    return builder()
        .withSubject(
            sb ->
                sb.elementArray(
                    "names",
                    n1 -> n1.string("use", "John").stringArray("given", "John", "Johnny"),
                    n2 -> n2.string("use", "Jane").stringArray("given", "Jane", "Janey")))
        .group("union() deduplicates within each iteration element")
        .testEquals(
            List.of("John", "Johnny", "Jane", "Janey"),
            "names.select(use.union(given))",
            "union() inside select deduplicates overlapping use/given per name element")
        .testEquals(
            List.of("John", "Johnny", "Jane", "Janey"),
            "names.select(use | given)",
            "`|` inside select deduplicates overlapping use/given per name element (control)")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testCombineIterationContextInsideSelect() {
    return builder()
        .withSubject(
            sb ->
                sb.elementArray(
                    "names",
                    n1 -> n1.string("use", "John").stringArray("given", "John", "Johnny"),
                    n2 -> n2.string("use", "Jane").stringArray("given", "Jane", "Janey")))
        .group(
            "combine() honours the surrounding iteration context and preserves duplicates"
                + " per element")
        .testEquals(
            List.of("John", "John", "Johnny", "Jane", "Jane", "Janey"),
            "names.select(use.combine(given))",
            "Function form: `given` resolves per-name, and combine() preserves duplicates")
        .build();
  }

  // ---------------------------------------------------------------------------------------------
  // Error paths: incompatible polymorphic types.
  // ---------------------------------------------------------------------------------------------

  @FhirPathTest
  public Stream<DynamicTest> testUnionIncompatibleTypesRaisesError() {
    return builder()
        .withSubject(sb -> sb.integer("i", 1).bool("b", true).string("s", "x"))
        .group("union() of incompatible polymorphic types raises an error")
        .testError("i.union(b)", "Integer.union(Boolean) is not supported")
        .testError("b.union(i)", "Boolean.union(Integer) is not supported")
        .testError("s.union(i)", "String.union(Integer) is not supported")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testCombineIncompatibleTypesRaisesError() {
    return builder()
        .withSubject(sb -> sb.integer("i", 1).bool("b", true).string("s", "x"))
        .group("combine() of incompatible polymorphic types raises an error")
        .testError("i.combine(b)", "Integer.combine(Boolean) is not supported")
        .testError("b.combine(i)", "Boolean.combine(Integer) is not supported")
        .testError("s.combine(i)", "String.combine(Integer) is not supported")
        .build();
  }

  // ---------------------------------------------------------------------------------------------
  // Arity errors: raised at parse time by the desugaring visitor.
  // ---------------------------------------------------------------------------------------------

  @FhirPathTest
  public Stream<DynamicTest> testUnionArityErrors() {
    return builder()
        .withSubject(sb -> sb.integerArray("ints", 1, 2, 3))
        .group("union() with wrong argument count is rejected at parse time")
        .testError("ints.union()", "union() with zero arguments is rejected")
        .testError("ints.union(1, 2)", "union() with two arguments is rejected")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testCombineArityErrors() {
    return builder()
        .withSubject(sb -> sb.integerArray("ints", 1, 2, 3))
        .group("combine() with wrong argument count is rejected at parse time")
        .testError("ints.combine()", "combine() with zero arguments is rejected")
        .testError("ints.combine(1, 2)", "combine() with two arguments is rejected")
        .build();
  }
}
