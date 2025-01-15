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

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import au.csiro.pathling.fhirpath.annotations.NotImplemented;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import au.csiro.pathling.fhirpath.execution.CollectionDataset;
import jakarta.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.StructType;

/**
 * @author Piotr Szul
 * @author John Grimes
 */
@SuppressWarnings("unused")
@NotImplemented
public abstract class BaseFhirPathAssertion<T extends BaseFhirPathAssertion<T>> {

  @Nonnull
  protected final CollectionDataset datasetResult;
  protected final Collection result;

  BaseFhirPathAssertion(@Nonnull final CollectionDataset datasetResult) {
    this.datasetResult = datasetResult;
    this.result = datasetResult.getValue();
  }

  // TODO: implement with columns


  @Nonnull
  protected Column getValueColumn() {
    return result.getColumnValue();
  }


  @Nonnull
  public DatasetAssert toCanonicalResult() {
    return DatasetAssert.of(datasetResult.toCanonical().toIdValueDataset());
  }

  @Nonnull
  public DatasetAssert selectResult() {
    final Dataset<Row> resultDataset = datasetResult.getDataset()
        .select(functions.col("id"), getValueColumn().alias("value"));
    final StructType schema = resultDataset.schema();
    final Dataset<Row> explodedDataset = schema.fields()[schema.fieldIndex(
        "value")].dataType() instanceof ArrayType
                                         ? resultDataset.select(resultDataset.col("id"),
        functions.explode_outer(resultDataset.col("value"))
            .alias("value"))
                                         : resultDataset;

    return DatasetAssert.of(explodedDataset);
  }

  @Nonnull
  public DatasetAssert selectOrderedResult() {
    final Dataset<Row> resultDataset = datasetResult.getDataset()
        .select(functions.col("id"), getValueColumn().alias("value"));
    final StructType schema = resultDataset.schema();
    final Dataset<Row> explodedDataset = schema.fields()[schema.fieldIndex(
        "value")].dataType() instanceof ArrayType
                                         ? resultDataset.select(resultDataset.col("id"),
        functions.explode_outer(resultDataset.col("value"))
            .alias("value"))
                                         : resultDataset;

    return DatasetAssert.of(explodedDataset.orderBy("id"));
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
    // fail("Not implemented: hasExpression");
    // assertEquals(expression, result.getExpression());
    return self();
  }

  public T isSingular() {
    // TODO: not implemented
    //fail("Not implemented: isSingluar");
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
    assertTrue(ofType.isAssignableFrom(result.getClass()),
        ofType.getName() + " is not assignable from " + result.getClass().getName());
    return new ElementPathAssertion(datasetResult);
  }

  public ResourcePathAssertion isResourcePath() {
    assertTrue(ResourceCollection.class.isAssignableFrom(result.getClass()));
    return new ResourcePathAssertion((ResourceCollection) result, datasetResult);
  }

  public LiteralPathAssertion isLiteralPath(final Class<? extends Collection> ofType) {
    assertTrue(ofType.isAssignableFrom(result.getClass()));
    assertInstanceOf(Literal.class, result.getColumnValue().expr());
    return new LiteralPathAssertion(datasetResult);
  }

  @SuppressWarnings("unchecked")
  private T self() {
    return (T) this;
  }
}
