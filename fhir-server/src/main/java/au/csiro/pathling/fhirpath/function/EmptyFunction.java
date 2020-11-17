/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function;

import static au.csiro.pathling.fhirpath.function.NamedFunction.checkNoArguments;
import static au.csiro.pathling.fhirpath.function.NamedFunction.expressionFromInput;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.NonLiteralPath;
import au.csiro.pathling.fhirpath.element.ElementPath;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * This function returns true if the input collection is empty.
 *
 * @author John Grimes
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#empty">empty</a>
 */
public class EmptyFunction implements NamedFunction {

  private static final String NAME = "empty";

  @Nonnull
  @Override
  public FhirPath invoke(@Nonnull final NamedFunctionInput input) {
    checkNoArguments(NAME, input);
    final NonLiteralPath inputPath = input.getInput();
    final String expression = expressionFromInput(input, "empty");

    // We use the count function to determine whether there are zero items in the input collection.
    final FhirPath countResult = new CountFunction().invoke(input);
    final Dataset<Row> dataset = countResult.getDataset();
    final Column valueColumn = countResult.getValueColumn().equalTo(0);

    return ElementPath
        .build(expression, dataset, inputPath.getIdColumn(), valueColumn, true, Optional.empty(),
            inputPath.getThisColumn(), FHIRDefinedType.BOOLEAN);
  }

}
