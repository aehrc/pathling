/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test.assertions;

import static au.csiro.pathling.utilities.Preconditions.check;
import static au.csiro.pathling.utilities.Preconditions.checkPresent;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.NonLiteralPath;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.literal.LiteralPath;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;

/**
 * @author Piotr Szul
 * @author John Grimes
 */
@SuppressWarnings("unused")
public class FhirPathAssertion<T extends FhirPathAssertion> {

  @Nonnull
  private final FhirPath fhirPath;

  FhirPathAssertion(@Nonnull final FhirPath fhirPath) {
    this.fhirPath = fhirPath;
  }

  @Nonnull
  public DatasetAssert selectResult() {
    check(fhirPath.getIdColumn().isPresent());
    return new DatasetAssert(fhirPath.getDataset()
        .select(fhirPath.getIdColumn().get(), fhirPath.getValueColumn())
        .orderBy(fhirPath.getIdColumn().get(), fhirPath.getValueColumn()));
  }


  @Nonnull
  public DatasetAssert selectGroupingResult(@Nonnull final List<Column> groupingColumns) {
    return selectGroupingResult(groupingColumns, false);
  }

  @Nonnull
  public DatasetAssert selectGroupingResult(@Nonnull final List<Column> groupingColumns,
      final boolean preserveOrder) {
    check(fhirPath.getIdColumn().isEmpty());
    check(!groupingColumns.isEmpty());
    final ArrayList<Column> allColumnsList = new ArrayList<>(groupingColumns);
    allColumnsList.add(fhirPath.getValueColumn());
    final Column[] allColumns = allColumnsList.toArray(new Column[0]);
    return new DatasetAssert(preserveOrder
                             ? fhirPath.getDataset().select(allColumns)
                             : fhirPath.getDataset().select(allColumns).orderBy(allColumns));
  }

  @Nonnull
  public DatasetAssert selectOrderedResult() {
    check(fhirPath.getIdColumn().isPresent());
    check(fhirPath instanceof NonLiteralPath);
    final NonLiteralPath nonLiteralPath = (NonLiteralPath) fhirPath;
    checkPresent(nonLiteralPath.getEidColumn());

    return new DatasetAssert(fhirPath.getDataset()
        .select(fhirPath.getIdColumn().get(), nonLiteralPath.getEidColumn().get(),
            fhirPath.getValueColumn())
        .orderBy(fhirPath.getIdColumn().get(), nonLiteralPath.getEidColumn().get()));
  }

  @Nonnull
  public DatasetAssert selectResultPreserveOrder() {
    check(fhirPath.getIdColumn().isPresent());
    return new DatasetAssert(fhirPath.getDataset()
        .select(fhirPath.getIdColumn().get(),
            fhirPath.getValueColumn()));
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

  public LiteralPathAssertion isLiteralPath(final Class<? extends LiteralPath> ofType) {
    assertTrue(ofType.isAssignableFrom(fhirPath.getClass()));
    return new LiteralPathAssertion((LiteralPath) fhirPath);
  }

  @SuppressWarnings("unchecked")
  private T self() {
    return (T) this;
  }

}
