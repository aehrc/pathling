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

import au.csiro.pathling.sql.DecimalNormalisation;
import au.csiro.pathling.sql.InstantNormalisation;
import au.csiro.pathling.sql.MergeCast;
import au.csiro.pathling.sql.QuantityNormalisation;
import au.csiro.pathling.sql.ResolveOrNull;
import au.csiro.pathling.sql.UnresolvedTraverseExtension;
import au.csiro.pathling.sql.UnresolvedTraverseRootExtension;
import au.csiro.pathling.utilities.CanonicalStructure;
import jakarta.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;
import lombok.experimental.UtilityClass;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.classic.ExpressionUtils;
import org.apache.spark.sql.types.DataType;
import scala.collection.immutable.Seq;

/**
 * Java-based utility class for creating Column expressions from Catalyst expressions. This class
 * uses Java to access package-private methods in Spark that are not accessible from Scala.
 */
@UtilityClass
public class ColumnFunctions {

  /**
   * Creates a Column from an array of Columns containing arrays of structs, producing an array of
   * structs where each element is a product of the elements of the input arrays.
   *
   * @param columns The input columns
   * @return A Column with the struct product
   */
  @Nonnull
  public static Column structProduct(@Nonnull final Column... columns) {
    // Convert columns to expressions using Java streams
    final List<Expression> expressionList =
        Arrays.stream(columns).map(ExpressionUtils::expression).toList();

    // Convert Java List to Scala Seq
    final Seq<Expression> expressions =
        scala.jdk.javaapi.CollectionConverters.asScala(expressionList).toSeq();

    // Create StructProduct expression
    final Expression structProductExpr = new StructProduct(expressions, false);

    // Convert back to Column
    return ExpressionUtils.column(structProductExpr);
  }

  /**
   * Creates a Column from an array of Columns containing arrays of structs, producing an array of
   * structs where each element is a product of the elements of the input arrays. If the input
   * arrays are of different lengths, the output array will contain nulls for missing elements in
   * the input.
   *
   * @param columns The input columns
   * @return A Column with the struct product (outer join style)
   */
  @Nonnull
  public static Column structProductOuter(@Nonnull final Column... columns) {
    // Convert columns to expressions using Java streams
    final List<Expression> expressionList =
        Arrays.stream(columns).map(ExpressionUtils::expression).toList();

    // Convert Java List to Scala Seq
    final Seq<Expression> expressions =
        scala.jdk.javaapi.CollectionConverters.asScala(expressionList).toSeq();

    // Create StructProduct expression with outer=true
    final Expression structProductExpr = new StructProduct(expressions, true);

    // Convert back to Column
    return ExpressionUtils.column(structProductExpr);
  }

  /**
   * Creates the tolerant traversal expression: a reference to a field of a structure, or of every
   * element of an array of structures, that resolves to a null of the fallback type where the
   * resolved input does not carry the field (FR-054).
   *
   * @param child the structure, or array of structures, to take the field from
   * @param fieldName the name of the field
   * @param fallback the type of the null returned when the field is absent, per FR-055
   * @return a Column that tolerates the absence of the field
   */
  @Nonnull
  public static Column resolveOrNull(
      @Nonnull final Column child,
      @Nonnull final String fieldName,
      @Nonnull final DataType fallback) {
    return ExpressionUtils.column(
        new ResolveOrNull(ExpressionUtils.expression(child), fieldName, fallback));
  }

  /**
   * Creates a reference to a table-level column that resolves to a null of the fallback type where
   * the input does not have the column, rather than failing (decision 75).
   *
   * @param columnName the name of the table-level column
   * @param fallback the type of the null returned when the column is absent
   * @return a Column that tolerates the absence of the table-level column
   */
  @Nonnull
  public static Column columnOrNull(
      @Nonnull final String columnName, @Nonnull final DataType fallback) {
    return ExpressionUtils.column(new UnresolvedColumnOrNull(columnName, fallback));
  }

  /**
   * Creates a reference to a table-level decimal column that resolves to its text on either layout,
   * and to a null of the fallback type where the input does not have the column. The previous
   * layout's {@code DECIMAL(32,6)} value is normalised to its text at the scale of the source,
   * which is read from the column's {@code _scale} companion (T094b).
   *
   * @param columnName the name of the table-level column
   * @param fallback the type of the null returned when the column is absent
   * @return a Column that yields the text of the decimal column
   */
  @Nonnull
  public static Column decimalColumnOrNull(
      @Nonnull final String columnName, @Nonnull final DataType fallback) {
    return ExpressionUtils.column(DecimalNormalisation.tolerantColumn(columnName, fallback));
  }

  /**
   * Creates a reference to a table-level instant column that resolves to its text on either layout,
   * and to a null of the fallback type where the input does not have the column. The previous
   * layout's timestamp is normalised to its text in UTC, which does not depend on the session time
   * zone (decision 81).
   *
   * @param columnName the name of the table-level column
   * @param fallback the type of the null returned when the column is absent
   * @return a Column that yields the text of the instant column
   */
  @Nonnull
  public static Column instantColumnOrNull(
      @Nonnull final String columnName, @Nonnull final DataType fallback) {
    return ExpressionUtils.column(InstantNormalisation.tolerantColumn(columnName, fallback));
  }

  /**
   * Creates a reference to a table-level quantity column that resolves to the new layout's shape on
   * either layout, and to a null of the fallback type where the input does not have the column. The
   * previous layout's quantity is normalised to a structure without its canonical form, whose value
   * is its text at the scale of the source (T094b).
   *
   * @param columnName the name of the table-level column
   * @param fallback the type of the null returned when the column is absent
   * @return a Column that yields the quantity in the new layout's shape
   */
  @Nonnull
  public static Column quantityColumnOrNull(
      @Nonnull final String columnName, @Nonnull final DataType fallback) {
    return ExpressionUtils.column(QuantityNormalisation.tolerantColumn(columnName, fallback));
  }

  /**
   * Creates the traversal to the extensions of an element, which reads the inline {@code extension}
   * field on the new layout and looks the element's {@code _fid} up in the {@code _extension}
   * column on the previous one.
   *
   * @param parent the structure, or array of structures, whose extensions are wanted
   * @return a Column holding the extensions of the parent
   */
  @Nonnull
  public static Column traverseExtension(@Nonnull final Column parent) {
    return ExpressionUtils.column(
        new UnresolvedTraverseExtension(ExpressionUtils.expression(parent)));
  }

  /**
   * Creates the traversal to the extensions of the resource itself, which reads the {@code
   * extension} column on the new layout and looks the resource's {@code _fid} up in the {@code
   * _extension} column on the previous one.
   *
   * @return a Column holding the extensions of the resource
   */
  @Nonnull
  public static Column traverseRootExtension() {
    return ExpressionUtils.column(new UnresolvedTraverseRootExtension());
  }

  /**
   * Creates the reconciliation expression, which projects one of several operands of the same FHIR
   * type by name into the recursive field-wise merge of all of their types (FR-056). Every operand
   * given the same list and canonical structure is projected into the same type.
   *
   * @param operands the full, ordered list of operands being reconciled
   * @param index the position in the list of the operand to project
   * @param canonical the canonical structure of the operands' element type
   * @return a Column holding the operand, projected into the merged type
   */
  @Nonnull
  public static Column mergeCast(
      @Nonnull final List<Column> operands,
      final int index,
      @Nonnull final CanonicalStructure canonical) {
    final Seq<Expression> expressions =
        scala.jdk.javaapi.CollectionConverters.asScala(
                operands.stream().map(ExpressionUtils::expression).toList())
            .toSeq();
    return ExpressionUtils.column(new MergeCast(expressions, index, canonical));
  }
}
