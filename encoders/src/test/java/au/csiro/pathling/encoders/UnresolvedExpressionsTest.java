/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright 2018-2025 Commonwealth Scientific
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
 *
 */

package au.csiro.pathling.encoders;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import jakarta.annotation.Nonnull;
import java.util.Arrays;
import org.apache.spark.sql.catalyst.analysis.UnresolvedException;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.types.DataTypes;
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

    assertEquals(
        new UnresolvedTransformTree(stringLiteral("data2"), extractor, toIndexedSeq(traversor), 2),
        unresolvedTransformTree.withNewChildrenInternal(toIndexedSeq(stringLiteral("data2"))));
  }
}
