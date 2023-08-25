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

import au.csiro.pathling.fhirpath.annotations.NotImplemented;
import au.csiro.pathling.fhirpath.collection.Collection;
import javax.annotation.Nonnull;

/**
 * @author Piotr Szul
 * @author John Grimes
 */
@SuppressWarnings("unused")
@NotImplemented
public abstract class BaseFhirPathAssertion<T extends BaseFhirPathAssertion<T>> {

  @Nonnull
  private final Collection result;

  BaseFhirPathAssertion(@Nonnull final Collection result) {
    this.result = result;
  }

  
  // TODO: implement with columns
  
  // @Nonnull
  // public DatasetAssert selectResult() {
  //   final Column[] selection = new Column[]{result.getIdColumn(), result.getValueColumn()};
  //   return new DatasetAssert(result.getDataset().select(selection));
  // }
  //
  // @Nonnull
  // public DatasetAssert selectOrderedResult() {
  //   final Column[] selection = new Column[]{result.getIdColumn(), result.getValueColumn()};
  //   // TODO: Update this to make sure that it is ordered.
  //   return new DatasetAssert(result.getDataset().select(selection));
  // }
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
  // @Nonnull
  // public T hasExpression(@Nonnull final String expression) {
  //   assertEquals(expression, result.getExpression());
  //   return self();
  // }
  //
  // public T isSingular() {
  //   assertTrue(result.isSingular());
  //   return self();
  // }
  //
  // public T isNotSingular() {
  //   assertFalse(result.isSingular());
  //   return self();
  // }
  //
  // public T preservesCardinalityOf(final Collection otherResult) {
  //   assertEquals(otherResult.isSingular(), result.isSingular());
  //   return self();
  // }
  //
  //
  // public ElementPathAssertion isElementPath(final Class<? extends PrimitivePath> ofType) {
  //   assertTrue(ofType.isAssignableFrom(result.getClass()));
  //   return new ElementPathAssertion((PrimitivePath) result);
  // }
  //
  // public ResourcePathAssertion isResourcePath() {
  //   assertTrue(ResourceCollection.class.isAssignableFrom(result.getClass()));
  //   return new ResourcePathAssertion((ResourceCollection) result);
  // }
  //
  // public LiteralPathAssertion isLiteralPath(final Class<? extends LiteralPath> ofType) {
  //   assertTrue(ofType.isAssignableFrom(result.getClass()));
  //   return new LiteralPathAssertion((LiteralPath) result);
  // }

  @SuppressWarnings("unchecked")
  private T self() {
    return (T) this;
  }
}
