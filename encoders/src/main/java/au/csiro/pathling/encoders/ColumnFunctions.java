/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright 2018-2025 Commonwealth Scientific and Industrial Research
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
 *
 */

package au.csiro.pathling.encoders;

import jakarta.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.classic.ExpressionUtils;
import scala.collection.JavaConverters;
import scala.collection.immutable.Seq;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Java-based utility class for creating Column expressions from Catalyst expressions.
 * This class uses Java to access package-private methods in Spark that are not accessible from Scala.
 */
public class ColumnFunctions {

  /**
   * Creates a Column from an array of Columns containing arrays of structs, producing
   * an array of structs where each element is a product of the elements of the input arrays.
   *
   * @param columns The input columns
   * @return A Column with the struct product
   */
  @Nonnull
  public static Column structProduct(@Nonnull final Column... columns) {
    // Convert columns to expressions using Java streams
    final List<Expression> expressionList = Arrays.stream(columns)
        .map(ExpressionUtils::expression)
        .collect(Collectors.toList());

    // Convert Java List to Scala Seq
    final Seq<Expression> expressions = JavaConverters.asScalaBuffer(expressionList).toSeq();

    // Create StructProduct expression
    final Expression structProductExpr = new StructProduct(expressions, false);

    // Convert back to Column
    return ExpressionUtils.column(structProductExpr);
  }

  /**
   * Creates a Column from an array of Columns containing arrays of structs, producing
   * an array of structs where each element is a product of the elements of the input arrays.
   * If the input arrays are of different lengths, the output array will contain nulls for missing
   * elements in the input.
   *
   * @param columns The input columns
   * @return A Column with the struct product (outer join style)
   */
  @Nonnull
  public static Column structProductOuter(@Nonnull final Column... columns) {
    // Convert columns to expressions using Java streams
    final List<Expression> expressionList = Arrays.stream(columns)
        .map(ExpressionUtils::expression)
        .collect(Collectors.toList());

    // Convert Java List to Scala Seq
    final Seq<Expression> expressions = JavaConverters.asScalaBuffer(expressionList).toSeq();

    // Create StructProduct expression with outer=true
    final Expression structProductExpr = new StructProduct(expressions, true);

    // Convert back to Column
    return ExpressionUtils.column(structProductExpr);
  }
}
