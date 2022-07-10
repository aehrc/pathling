/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function.memberof;

import static au.csiro.pathling.fhirpath.function.NamedFunction.expressionFromInput;
import static au.csiro.pathling.utilities.Preconditions.checkPresent;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;
import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.fhir.TerminologyServiceFactory;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.function.NamedFunction;
import au.csiro.pathling.fhirpath.function.NamedFunctionInput;
import au.csiro.pathling.fhirpath.literal.StringLiteralPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.terminology.TerminologyFunctions;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.slf4j.MDC;

/**
 * A function that takes a set of Codings or CodeableConcepts as inputs and returns a set of boolean
 * values, based upon whether each item is present within the ValueSet identified by the supplied
 * URL.
 *
 * @author John Grimes
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#memberof">memberOf</a>
 */
public class MemberOfFunction implements NamedFunction {

  private static final String NAME = "memberOf";

  /**
   * Returns a new instance of this function.
   */
  public MemberOfFunction() {
  }

  private boolean isCodeableConcept(@Nonnull final FhirPath fhirPath) {
    return (fhirPath instanceof ElementPath &&
        FHIRDefinedType.CODEABLECONCEPT.equals(((ElementPath) fhirPath).getFhirType()));
  }

  @Nonnull
  @Override
  public FhirPath invoke(@Nonnull final NamedFunctionInput input) {
    validateInput(input);
    final ElementPath inputPath = (ElementPath) input.getInput();
    final StringLiteralPath argument = (StringLiteralPath) input.getArguments().get(0);
    final ParserContext inputContext = input.getContext();

    final Column idColumn = inputPath.getIdColumn();
    final Column conceptColumn = inputPath.getValueColumn();

    final Column codingArrayCol = (isCodeableConcept(inputPath))
                                  ? conceptColumn.getField("coding")
                                  : when(conceptColumn.isNotNull(), array(conceptColumn))
                                      .otherwise(functions.lit(null));

    // Prepare the data which will be used within the map operation. All of these things must be
    // Serializable.
    final TerminologyServiceFactory terminologyServiceFactory =
        checkPresent(inputContext.getTerminologyServiceFactory());
    final String valueSetUri = argument.getValue().getValueAsString();
    final Dataset<Row> dataset = inputPath.getDataset();

    final Dataset<Row> resultDataset = TerminologyFunctions.memberOf(codingArrayCol, valueSetUri,
        dataset, "result", terminologyServiceFactory, MDC.get("requestId"));
    final Column resultColumn = functions.col("result");

    // Construct a new result expression.
    final String expression = expressionFromInput(input, NAME);

    return ElementPath
        .build(expression, resultDataset, idColumn, inputPath.getEidColumn(), resultColumn,
            inputPath.isSingular(), inputPath.getCurrentResource(), inputPath.getThisColumn(),
            FHIRDefinedType.BOOLEAN);
  }

  private void validateInput(@Nonnull final NamedFunctionInput input) {
    final ParserContext context = input.getContext();
    checkUserInput(context.getTerminologyServiceFactory()
        .isPresent(), "Attempt to call terminology function " + NAME
        + " when terminology service has not been configured");

    final FhirPath inputPath = input.getInput();
    checkUserInput(inputPath instanceof ElementPath
            && (((ElementPath) inputPath).getFhirType().equals(FHIRDefinedType.CODING)
            || ((ElementPath) inputPath).getFhirType().equals(FHIRDefinedType.CODEABLECONCEPT)),
        "Input to memberOf function is of unsupported type: " + inputPath.getExpression());
    checkUserInput(input.getArguments().size() == 1,
        "memberOf function accepts one argument of type String");
    final FhirPath argument = input.getArguments().get(0);
    checkUserInput(argument instanceof StringLiteralPath,
        "memberOf function accepts one argument of type String literal");
  }

}
