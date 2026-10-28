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

import au.csiro.pathling.sql.ExtensionTraversal;
import au.csiro.pathling.sql.MergeCast;
import au.csiro.pathling.sql.ResolveFieldsOrElse;
import au.csiro.pathling.sql.ResolveOrElse;
import au.csiro.pathling.sql.ResolveOrNull;
import au.csiro.pathling.sql.UnresolvedMergeCombination;
import au.csiro.pathling.sql.UnresolvedTraverseExtension;
import au.csiro.pathling.utilities.CanonicalStructure;
import jakarta.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import lombok.experimental.UtilityClass;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.classic.ColumnConversions$;
import org.apache.spark.sql.classic.ExpressionUtils;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.immutable.Seq;
import scala.jdk.javaapi.CollectionConverters;

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
   * Creates a reference to a field of a structure, or of every element of an array of structures,
   * that resolves to a fallback value where the resolved input does not carry the field. Presence
   * is decided once for the schema, never per row (FR-023).
   *
   * @param child the structure, or array of structures, to take the field from
   * @param fieldName the name of the field
   * @param fallback the value taken when the field is absent
   * @return the field, or the fallback
   */
  @Nonnull
  public static Column resolveOrElse(
      @Nonnull final Column child,
      @Nonnull final String fieldName,
      @Nonnull final Column fallback) {
    return ExpressionUtils.column(
        new ResolveOrElse(
            ExpressionUtils.expression(child), fieldName, ExpressionUtils.expression(fallback)));
  }

  /**
   * Creates a structure of several fields of a structure that resolves to a fallback structure
   * where the resolved input does not carry all of them, so that a fallback computing them all at
   * once is evaluated once. Presence is decided once for the schema, never per row (FR-023).
   *
   * @param child the structure that may carry the fields
   * @param fieldNames the names of the fields
   * @param fallback the structure of the same fields to take where any of them is absent
   * @return a structure of the fields, or the fallback
   */
  @Nonnull
  public static Column resolveFieldsOrElse(
      @Nonnull final Column child,
      @Nonnull final List<String> fieldNames,
      @Nonnull final Column fallback) {
    return ExpressionUtils.column(
        new ResolveFieldsOrElse(
            ExpressionUtils.expression(child),
            CollectionConverters.asScala(fieldNames).toSeq(),
            ExpressionUtils.expression(fallback)));
  }

  /**
   * Resolves a text field of a structure, or of every structure in an array, to a null string where
   * the structure does not carry the field, rather than failing (FR-054). The field is read as the
   * text it is stored as.
   *
   * @param child the structure, or array of structures
   * @param fieldName the name of the field
   * @return the field, or a null string where the structure does not carry it
   */
  @Nonnull
  public static Column resolveStringOrNull(
      @Nonnull final Column child, @Nonnull final String fieldName) {
    return resolveOrNull(child, fieldName, DataTypes.StringType);
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
   * Creates the traversal to the extensions of an element, which reads its inline {@code extension}
   * field, and finds no extensions where the element has none.
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
   * extension} column, and finds no extensions where the table has no such column.
   *
   * @return a Column holding the extensions of the resource
   */
  @Nonnull
  public static Column traverseRootExtension() {
    return columnOrNull(
        ExtensionSupport.EXTENSION_ELEMENT_NAME(), ExtensionTraversal.ABSENT_TYPE());
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

  /**
   * Combines several operands of the same FHIR type, each projected by name into the recursive
   * field-wise merge of all of their types (FR-056). Unlike {@link #mergeCast(List, int,
   * CanonicalStructure)}, each operand is held once in the combination, and an operand whose type
   * is already the merged type is left as it is.
   *
   * @param operands the full, ordered list of operands being combined
   * @param canonical the canonical structure of the operands' element type
   * @param combination the combination of the projected operands, given in the same order
   * @return a Column holding the combination of the projected operands
   */
  @Nonnull
  public static Column mergeCombination(
      @Nonnull final List<Column> operands,
      @Nonnull final CanonicalStructure canonical,
      @Nonnull final Function<List<Column>, Column> combination) {
    final Seq<Expression> expressions =
        scala.jdk.javaapi.CollectionConverters.asScala(
                operands.stream().map(ExpressionUtils::expression).toList())
            .toSeq();
    final scala.Function1<Seq<Expression>, Expression> combine =
        projected ->
            ColumnConversions$.MODULE$
                .toRichColumn(
                    combination.apply(
                        scala.jdk.javaapi.CollectionConverters.asJava(projected).stream()
                            .map(ExpressionUtils::column)
                            .toList()))
                .expr();
    return ExpressionUtils.column(new UnresolvedMergeCombination(expressions, canonical, combine));
  }
}
