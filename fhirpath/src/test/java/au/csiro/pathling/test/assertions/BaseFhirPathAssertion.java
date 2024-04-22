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

import static au.csiro.pathling.utilities.Preconditions.check;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.literal.LiteralPath;
import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.Column;
import org.hl7.fhir.r4.model.Type;

/**
 * @author Piotr Szul
 * @author John Grimes
 */
@SuppressWarnings("unused")
public abstract class BaseFhirPathAssertion<T extends BaseFhirPathAssertion<T>> {

  @Nonnull
  private final FhirPath fhirPath;

  BaseFhirPathAssertion(@Nonnull final FhirPath fhirPath) {
    this.fhirPath = fhirPath;
  }

  @Nonnull
  public DatasetAssert selectResult() {
    final Column[] selection = new Column[]{fhirPath.getIdColumn(), fhirPath.getValueColumn()};
    return new DatasetAssert(fhirPath.getDataset().select(selection));
  }

  @Nonnull
  public DatasetAssert selectOrderedResult() {
    final Column[] selection = new Column[]{fhirPath.getIdColumn(), fhirPath.getValueColumn()};
    final Column[] ordering = new Column[]{fhirPath.getIdColumn(), fhirPath.getOrderingColumn()};

    return new DatasetAssert(fhirPath.getOrderedDataset()
        .select(selection)
        .orderBy(ordering));
  }

  @Nonnull
  public DatasetAssert selectGroupingResult(@Nonnull final List<Column> groupingColumns) {
    return selectGroupingResult(groupingColumns, false);
  }

  @Nonnull
  @SuppressWarnings("SameParameterValue")
  private DatasetAssert selectGroupingResult(@Nonnull final List<Column> groupingColumns,
      final boolean preserveOrder) {
    check(!groupingColumns.isEmpty());
    final ArrayList<Column> allColumnsList = new ArrayList<>(groupingColumns);
    allColumnsList.add(fhirPath.getValueColumn());
    final Column[] allColumns = allColumnsList.toArray(new Column[0]);
    return new DatasetAssert(preserveOrder
                             ? fhirPath.getDataset().select(allColumns)
                             : fhirPath.getDataset().select(allColumns).orderBy(allColumns));
  }

  @Nonnull
  public DatasetAssert selectOrderedResultWithEid() {
    final Column[] selection = new Column[]{fhirPath.getIdColumn(), fhirPath.getOrderingColumn(),
        fhirPath.getValueColumn()};
    final Column[] ordering = new Column[]{fhirPath.getIdColumn(), fhirPath.getOrderingColumn()};

    return new DatasetAssert(fhirPath.getOrderedDataset()
        .select(selection)
        .orderBy(ordering));
  }

  @Nonnull
  public T hasExpression(@Nonnull final String expression) {
    assertEquals(expression, fhirPath.getExpression());
    return self();
  }

  public T isSingular() {
    assertTrue(fhirPath.isSingular());
    return self();
  }

  public T isNotSingular() {
    assertFalse(fhirPath.isSingular());
    return self();
  }

  public T preservesCardinalityOf(final FhirPath otherFhirPath) {
    assertEquals(otherFhirPath.isSingular(), fhirPath.isSingular());
    return self();
  }


  public ElementPathAssertion isElementPath(final Class<? extends ElementPath> ofType) {
    assertTrue(ofType.isAssignableFrom(fhirPath.getClass()));
    return new ElementPathAssertion((ElementPath) fhirPath);
  }

  public ResourcePathAssertion isResourcePath() {
    assertTrue(ResourcePath.class.isAssignableFrom(fhirPath.getClass()));
    return new ResourcePathAssertion((ResourcePath) fhirPath);
  }

  public LiteralPathAssertion isLiteralPath(
      final Class<? extends LiteralPath<? extends Type>> ofType) {
    assertTrue(ofType.isAssignableFrom(fhirPath.getClass()));
    return new LiteralPathAssertion((LiteralPath<? extends Type>) fhirPath);
  }

  @SuppressWarnings("unchecked")
  private T self() {
    return (T) this;
  }
}
