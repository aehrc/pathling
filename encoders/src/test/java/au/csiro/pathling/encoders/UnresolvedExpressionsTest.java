/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright 2018-2026 Commonwealth Scientific
 * and Industrial Research Organisation (CSIRO) ABN 41 687 119 230.
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
package au.csiro.pathling.encoders;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.annotation.Nonnull;
import java.util.Arrays;
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute;
import org.apache.spark.sql.catalyst.analysis.UnresolvedException;
import org.apache.spark.sql.catalyst.expressions.ArrayTransform;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.LambdaFunction;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.expressions.variant.VariantGet;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.VariantType$;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;
import scala.Function1;
import scala.collection.immutable.IndexedSeq;
import scala.jdk.javaapi.CollectionConverters;

class UnresolvedExpressionsTest {

  private static Expression stringLiteral(@Nonnull final String value) {
    return new Literal(UTF8String.fromString(value), DataTypes.StringType);
  }

  @SafeVarargs
  private static <T> IndexedSeq<T> toIndexedSeq(@Nonnull final T... expressions) {
    return CollectionConverters.asScala(Arrays.asList(expressions)).toIndexedSeq();
  }

  static void assertUnresolvedExpression(@Nonnull final Expression expression) {
    assertThrows(UnresolvedException.class, expression::nullable);
    assertThrows(UnresolvedException.class, expression::dataType);
    assertFalse(expression.resolved());
  }

  @Test
  void testUnresolvedNullIfMissingField() {
    final UnresolvedNullIfMissingField unresolvedNullIfMissingField =
        new UnresolvedNullIfMissingField(stringLiteral("data1"));
    assertUnresolvedExpression(unresolvedNullIfMissingField);
    assertEquals("data1", unresolvedNullIfMissingField.toString());
    assertEquals(
        new UnresolvedNullIfMissingField(stringLiteral("data2")),
        unresolvedNullIfMissingField.withNewChildrenInternal(toIndexedSeq(stringLiteral("data2"))));
  }

  @Test
  void testUnresolvedTransformTree() {

    final Function1<Expression, Expression> extractor = x -> x;
    final Function1<Expression, Expression> traversor = x -> x;

    final UnresolvedTransformTree unresolvedTransformTree =
        new UnresolvedTransformTree(stringLiteral("data1"), extractor, toIndexedSeq(traversor), 2);
    assertUnresolvedExpression(unresolvedTransformTree);
    assertEquals("data1", unresolvedTransformTree.toString());
    assertFalse(unresolvedTransformTree.errorOnDepthExhaustion());

    assertEquals(
        new UnresolvedTransformTree(stringLiteral("data2"), extractor, toIndexedSeq(traversor), 2),
        unresolvedTransformTree.withNewChildrenInternal(toIndexedSeq(stringLiteral("data2"))));
  }

  @Test
  void testUnresolvedTransformTreeWithErrorOnDepthExhaustion() {

    final Function1<Expression, Expression> extractor = x -> x;
    final Function1<Expression, Expression> traversor = x -> x;

    // Construct with errorOnDepthExhaustion = true.
    final UnresolvedTransformTree tree =
        new UnresolvedTransformTree(
            stringLiteral("data1"),
            extractor,
            toIndexedSeq(traversor),
            scala.Option.empty(),
            2,
            true);
    assertUnresolvedExpression(tree);
    assertTrue(tree.errorOnDepthExhaustion());

    // The flag should be preserved through withNewChildrenInternal.
    final Expression rebuilt = tree.withNewChildrenInternal(toIndexedSeq(stringLiteral("data2")));
    assertTrue(((UnresolvedTransformTree) rebuilt).errorOnDepthExhaustion());
  }

  private static Expression unresolvedAttribute(@Nonnull final String name) {
    return UnresolvedAttribute.quoted(name);
  }

  private static Expression variantArrayLiteral() {
    return Literal.create(null, DataTypes.createArrayType(VariantType$.MODULE$));
  }

  private static Expression structArrayLiteral() {
    return Literal.create(
        null,
        DataTypes.createArrayType(
            new StructType(
                new StructField[] {
                  DataTypes.createStructField("id", DataTypes.StringType, true)
                })));
  }

  private static Expression structLiteral() {
    return Literal.create(
        null,
        new StructType(
            new StructField[] {DataTypes.createStructField("name", DataTypes.StringType, true)}));
  }

  /** Extracts the {@link VariantGet} expression from an {@link ArrayTransform} result. */
  @Nonnull
  private static VariantGet extractVariantGet(@Nonnull final Expression resolved) {
    final ArrayTransform transform = assertInstanceOf(ArrayTransform.class, resolved);
    final LambdaFunction lambda = assertInstanceOf(LambdaFunction.class, transform.function());
    return assertInstanceOf(VariantGet.class, lambda.function());
  }

  @Test
  void testUnresolvedVariantUnwrap() {
    final UnresolvedVariantUnwrap unwrap =
        new UnresolvedVariantUnwrap(
            stringLiteral("inner"),
            stringLiteral("schema"),
            UnresolvedVariantUnwrap.apply$default$3());
    assertUnresolvedExpression(unwrap);
    assertEquals("VariantUnwrap(inner)", unwrap.toString());

    // withNewChildrenInternal produces a correct copy.
    final Expression rebuilt =
        unwrap.withNewChildrenInternal(
            toIndexedSeq(stringLiteral("newInner"), stringLiteral("newSchema")));
    final UnresolvedVariantUnwrap rebuiltUnwrap =
        assertInstanceOf(UnresolvedVariantUnwrap.class, rebuilt);
    assertEquals("VariantUnwrap(newInner)", rebuiltUnwrap.toString());
  }

  @Test
  void testUnresolvedVariantUnwrapFailOnErrorPreserved() {
    // Construct with failOnError = false.
    final UnresolvedVariantUnwrap unwrap =
        new UnresolvedVariantUnwrap(stringLiteral("inner"), stringLiteral("schema"), false);
    assertFalse(unwrap.failOnError());

    // The flag should survive withNewChildrenInternal.
    final Expression rebuilt =
        unwrap.withNewChildrenInternal(toIndexedSeq(stringLiteral("a"), stringLiteral("b")));
    assertFalse(((UnresolvedVariantUnwrap) rebuilt).failOnError());
  }

  @Test
  void testUnresolvedVariantUnwrapMapChildrenUnresolved() {
    // When children remain unresolved, mapChildren returns an unresolved copy.
    final UnresolvedVariantUnwrap unwrap =
        new UnresolvedVariantUnwrap(
            unresolvedAttribute("col1"),
            unresolvedAttribute("col2"),
            UnresolvedVariantUnwrap.apply$default$3());

    final Expression result = unwrap.mapChildren(x -> x);
    assertInstanceOf(UnresolvedVariantUnwrap.class, result);
    assertFalse(result.resolved());
  }

  @Test
  void testUnresolvedVariantUnwrapMapChildrenResolvedWithArrayType() {
    // When both children are resolved and schemaRef is an ArrayType, the element type is
    // extracted and used as the target type for VariantGet.
    final UnresolvedVariantUnwrap unwrap =
        new UnresolvedVariantUnwrap(
            variantArrayLiteral(), structArrayLiteral(), UnresolvedVariantUnwrap.apply$default$3());

    final Expression resolved = unwrap.mapChildren(x -> x);
    final VariantGet variantGet = extractVariantGet(resolved);

    // The target type should be the element type of the array, not the array itself.
    final StructType expectedElementType =
        new StructType(
            new StructField[] {DataTypes.createStructField("id", DataTypes.StringType, true)});
    assertEquals(expectedElementType, variantGet.targetType());
    assertTrue(variantGet.failOnError());
  }

  @Test
  void testUnresolvedVariantUnwrapMapChildrenResolvedWithNonArrayType() {
    // When schemaRef is a non-ArrayType (e.g. StructType), it is used directly as the target
    // type for VariantGet.
    final UnresolvedVariantUnwrap unwrap =
        new UnresolvedVariantUnwrap(
            variantArrayLiteral(), structLiteral(), UnresolvedVariantUnwrap.apply$default$3());

    final Expression resolved = unwrap.mapChildren(x -> x);
    final VariantGet variantGet = extractVariantGet(resolved);

    final StructType expectedType =
        new StructType(
            new StructField[] {DataTypes.createStructField("name", DataTypes.StringType, true)});
    assertEquals(expectedType, variantGet.targetType());
  }

  @Test
  void testUnresolvedVariantUnwrapMapChildrenFailOnErrorFalse() {
    // failOnError=false should propagate into the generated VariantGet.
    final UnresolvedVariantUnwrap unwrap =
        new UnresolvedVariantUnwrap(variantArrayLiteral(), structArrayLiteral(), false);

    final Expression resolved = unwrap.mapChildren(x -> x);
    final VariantGet variantGet = extractVariantGet(resolved);
    assertFalse(variantGet.failOnError());
  }
}
