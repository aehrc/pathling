/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test.assertions;

import au.csiro.pathling.fhirpath.UntypedResourcePath;
import java.util.Arrays;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;

/**
 * @author John Grimes
 */
@SuppressWarnings("UnusedReturnValue")
public class UntypedResourcePathAssertion extends FhirPathAssertion<UntypedResourcePathAssertion> {

  @Nonnull
  private final UntypedResourcePath fhirPath;

  UntypedResourcePathAssertion(@Nonnull final UntypedResourcePath fhirPath) {
    super(fhirPath);
    this.fhirPath = fhirPath;
  }

  @Nonnull
  public DatasetAssert selectUntypedResourceResult() {
    final Column[] selection = Arrays
        .asList(fhirPath.getIdColumn(), fhirPath.getTypeColumn(), fhirPath.getValueColumn())
        .toArray(new Column[0]);
    return new DatasetAssert(fhirPath.getOrderedDataset()
        .select(selection)
        .orderBy(selection));
  }
 
}
