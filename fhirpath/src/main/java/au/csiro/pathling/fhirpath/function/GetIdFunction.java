package au.csiro.pathling.fhirpath.function;

import static au.csiro.pathling.QueryHelpers.createColumn;
import static au.csiro.pathling.fhirpath.function.NamedFunction.checkNoArguments;
import static au.csiro.pathling.fhirpath.function.NamedFunction.expressionFromInput;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;

import au.csiro.pathling.QueryHelpers.DatasetWithColumn;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.PrimitivePath;
import au.csiro.pathling.fhirpath.collection.ReferencePath;
import javax.annotation.Nonnull;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

public class GetIdFunction implements NamedFunction {

  private static final String NAME = "getId";

  @Nonnull
  @Override
  public Collection invoke(@Nonnull final NamedFunctionInput input) {
    checkNoArguments(NAME, input);
    checkUserInput(input.getInput() instanceof ReferencePath, "Input to getId must be a Reference");
    final ReferencePath referencePath = (ReferencePath) input.getInput();
    final DatasetWithColumn datasetWithColumn = createColumn(referencePath.getDataset(),
        referencePath.getReferenceIdColumn());
    final String expression = expressionFromInput(input, NAME, input.getInput());

    return PrimitivePath.build(expression, datasetWithColumn.getDataset(),
        referencePath.getIdColumn(), datasetWithColumn.getColumn(),
        referencePath.getOrderingColumn(), referencePath.isSingular(),
        referencePath.getCurrentResource(), referencePath.getThisColumn(), FHIRDefinedType.STRING);
  }

}
