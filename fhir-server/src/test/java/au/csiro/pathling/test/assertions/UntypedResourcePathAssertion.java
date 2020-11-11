/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test.assertions;

import static au.csiro.pathling.utilities.Preconditions.checkPresent;
import static org.junit.jupiter.api.Assertions.assertEquals;

import au.csiro.pathling.fhirpath.UntypedResourcePath;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

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
    final Column idColumn = checkPresent(fhirPath.getIdColumn());
    final List<Column> selection = new ArrayList<>();
    selection.add(idColumn);
    selection.add(fhirPath.getTypeColumn());
    selection.addAll(fhirPath.getValueColumns());
    final Column[] selectionArray = selection.toArray(new Column[0]);
    return new DatasetAssert(fhirPath.getDataset()
        .select(selectionArray)
        .orderBy(selectionArray));
  }

  @Nonnull
  public UntypedResourcePathAssertion hasPossibleTypes(@Nonnull final ResourceType... types) {
    final HashSet<Object> typeSet = new HashSet<>(Arrays.asList(types));
    assertEquals(typeSet, fhirPath.getPossibleTypes());
    return this;
  }

}
