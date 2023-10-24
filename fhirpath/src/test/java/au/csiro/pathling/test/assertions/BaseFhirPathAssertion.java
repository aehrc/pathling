/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.test.assertions;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import au.csiro.pathling.fhirpath.EvaluationContext;
import au.csiro.pathling.fhirpath.annotations.NotImplemented;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.StructType;
import javax.annotation.Nonnull;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author Piotr Szul
 * @author John Grimes
 */
@SuppressWarnings("unused")
@NotImplemented
public abstract class BaseFhirPathAssertion<T extends BaseFhirPathAssertion<T>> {

  @Nonnull
  protected final Collection result;

  @Nonnull
  private final EvaluationContext evaluationContext;

  BaseFhirPathAssertion(@Nonnull final Collection result,
      @Nonnull final EvaluationContext evaluationContext) {
    this.result = result;
    this.evaluationContext = evaluationContext;
  }

  // TODO: implement with columns

  @Nonnull
  public DatasetAssert selectResult() {
    final Column[] selection = new Column[]{
        evaluationContext.getResource().traverse("id").get().getColumn().alias("id"),
        result.getColumn().alias("value")
    };
    // and exploded the result if needed to compare with CSV 
    final Dataset<Row> resultDataset = evaluationContext.getDataset()
        .select(selection);
    final StructType schema = resultDataset.schema();
    final Dataset<Row> explodedDataset = schema.fields()[schema.fieldIndex(
        "value")].dataType() instanceof ArrayType
                                         ? resultDataset.select(resultDataset.col("id"),
        functions.explode(resultDataset.col("value"))
            .alias("value"))
                                         : resultDataset;

    return DatasetAssert.of(explodedDataset);
  }

  @Nonnull
  public DatasetAssert selectOrderedResult() {

    //
    final Column[] selection = new Column[]{
        evaluationContext.getResource().traverse("id").get().getColumn(),
        result.getColumn()
    };
    // TODO: Update this to make sure that it is ordered
    // and exploded is needed
    return DatasetAssert.of(evaluationContext.getDataset().select(selection));
  }

  //
  // @Nonnull
  // public DatasetAssert selectGroupingResult(@Nonnull final List<Column> groupingColumns) {
  //   return selectGroupingResult(groupingColumns, false);
  // }
  //
  // @Nonnull
  // private DatasetAssert selectGroupingResult(@Nonnull final List<Column> groupingColumns,
  //     final boolean preserveOrder) {
  //   check(!groupingColumns.isEmpty());
  //   final ArrayList<Column> allColumnsList = new ArrayList<>(groupingColumns);
  //   allColumnsList.add(result.getValueColumn());
  //   final Column[] allColumns = allColumnsList.toArray(new Column[0]);
  //   return new DatasetAssert(preserveOrder
  //                            ? result.getDataset().select(allColumns)
  //                            : result.getDataset().select(allColumns).orderBy(allColumns));
  // }
  //
  // @Nonnull
  // public DatasetAssert selectOrderedResultWithEid() {
  //   final Column[] selection = new Column[]{result.getIdColumn(), result.getValueColumn()};
  //   // TODO: Update this to make sure that it is ordered.
  //   return new DatasetAssert(result.getDataset().select(selection));
  // }
  //
  @Nonnull
  public T hasExpression(@Nonnull final String expression) {
    // TODO: not implemented
    fail("Not implemented: hasExpression");
    // assertEquals(expression, result.getExpression());
    return self();
  }

  public T isSingular() {
    // TODO: not implemented
    fail("Not implemented: isSingluar");
    // assertTrue(result.isSingular());
    return self();
  }

  public T isNotSingular() {
    // TODO: not implemented
    fail("Not implemented: isNotSingular");
    // assertFalse(result.isSingular());
    return self();
  }
  //
  // public T preservesCardinalityOf(final Collection otherResult) {
  //   assertEquals(otherResult.isSingular(), result.isSingular());
  //   return self();
  // }
  //

  public ElementPathAssertion isElementPath(final Class<? extends Collection> ofType) {
    assertTrue(ofType.isAssignableFrom(result.getClass()));
    return new ElementPathAssertion(result, evaluationContext);
  }

  public ResourcePathAssertion isResourcePath() {
    assertTrue(ResourceCollection.class.isAssignableFrom(result.getClass()));
    return new ResourcePathAssertion((ResourceCollection) result, evaluationContext);
  }

  public LiteralPathAssertion isLiteralPath(final Class<? extends Collection> ofType) {
    assertTrue(ofType.isAssignableFrom(result.getClass()));
    assertTrue(result.getColumn().expr() instanceof Literal);
    return new LiteralPathAssertion(result, evaluationContext);
  }

  @SuppressWarnings("unchecked")
  private T self() {
    return (T) this;
  }
}
