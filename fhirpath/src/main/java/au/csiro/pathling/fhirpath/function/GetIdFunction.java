package au.csiro.pathling.fhirpath.function;

import au.csiro.pathling.fhirpath.annotations.Name;
import au.csiro.pathling.fhirpath.annotations.NotImplemented;

@Name("getId")
@NotImplemented
public class GetIdFunction implements NamedFunction {

  // TODO: impelement as columns

  // private static final String NAME = "getId";
  //
  // @Nonnull
  // @Override
  // public Collection invoke(@Nonnull final NamedFunctionInput input) {
  //   checkNoArguments(NAME, input);
  //   checkUserInput(input.getInput() instanceof ReferencePath, "Input to getId must be a Reference");
  //   final ReferencePath referencePath = (ReferencePath) input.getInput();
  //   final DatasetWithColumn datasetWithColumn = createColumn(referencePath.getDataset(),
  //       referencePath.getReferenceIdColumn());
  //   final String expression = expressionFromInput(input, NAME, input.getInput());
  //
  //   return PrimitivePath.build(expression, datasetWithColumn.getDataset(),
  //       referencePath.getIdColumn(), datasetWithColumn.getColumn(),
  //       referencePath.getOrderingColumn(), referencePath.isSingular(),
  //       referencePath.getCurrentResource(), referencePath.getThisColumn(), FHIRDefinedType.STRING);
  // }

}
