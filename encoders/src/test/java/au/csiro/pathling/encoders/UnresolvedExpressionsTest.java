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
    final UnresolvedNullIfMissingField unresolvedNullIfMissingField = new UnresolvedNullIfMissingField(
        stringLiteral("data1")
    );
    assertUnresolvedExpression(unresolvedNullIfMissingField);
    assertEquals("data1", unresolvedNullIfMissingField.toString());
    assertEquals(new UnresolvedNullIfMissingField(stringLiteral("data2")),
        unresolvedNullIfMissingField.withNewChildrenInternal(toIndexedSeq(stringLiteral("data2")))
    );
  }

  @Test
  void testUnresolvedTransformTree() {

    final Function1<Expression, Expression> extractor = x -> x;
    final Function1<Expression, Expression> traversor = x -> x;

    final UnresolvedTransformTree unresolvedTransformTree = new UnresolvedTransformTree(
        stringLiteral("data1"), extractor, toIndexedSeq(traversor), 2
    );
    assertUnresolvedExpression(unresolvedTransformTree);
    assertEquals("data1", unresolvedTransformTree.toString());

    assertEquals(
        new UnresolvedTransformTree(
            stringLiteral("data2"), extractor, toIndexedSeq(traversor), 2),
        unresolvedTransformTree.withNewChildrenInternal(toIndexedSeq(stringLiteral("data2")))
    );
  }
}
