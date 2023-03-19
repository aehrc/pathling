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

package au.csiro.pathling.fhirpath.function;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.NonLiteralPath;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.literal.StringLiteralPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.StringType;

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.List;

import static au.csiro.pathling.fhirpath.TerminologyUtils.getCodingColumn;
import static au.csiro.pathling.fhirpath.function.NamedFunction.checkNoArguments;
import static au.csiro.pathling.fhirpath.function.NamedFunction.expressionFromInput;
import static au.csiro.pathling.sql.Terminology.display;
import static au.csiro.pathling.sql.Terminology.member_of;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;

/**
 * This function returns the display name for given Coding
 *
 * @author Piotr Szul
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#display">display</a>
 */
public class DisplayFunction implements NamedFunction {

  private static final String NAME = "display";

  @Nonnull
  @Override
  public FhirPath invoke(@Nonnull final NamedFunctionInput input) {

    validateInput(input);
    final ElementPath inputPath = (ElementPath) input.getInput();
    final String expression = expressionFromInput(input, NAME);

    final Arguments arguments = Arguments.of(input);
    var acceptLanguageStringType = arguments.getNullableValue(0, StringType.class);
    String acceptLanguage = null;
    if (acceptLanguageStringType != null) {
    	acceptLanguage = acceptLanguageStringType.asStringValue();
    } else {
    	acceptLanguage = null;
    }
    
    final Dataset<Row> dataset = inputPath.getDataset();
    final Column resultColumn = display(inputPath.getValueColumn(), acceptLanguage);

    return ElementPath
        .build(expression, dataset, inputPath.getIdColumn(), inputPath.getEidColumn(),
            resultColumn, inputPath.isSingular(), inputPath.getCurrentResource(),
            inputPath.getThisColumn(), FHIRDefinedType.STRING);
  }

  private void validateInput(@Nonnull final NamedFunctionInput input) {
    final ParserContext context = input.getContext();
    checkUserInput(context.getTerminologyServiceFactory()
        .isPresent(), "Attempt to call terminology function " + NAME
        + " when terminology service has not been configured");

    final FhirPath inputPath = input.getInput();
    checkUserInput(inputPath instanceof ElementPath
            && (((ElementPath) inputPath).getFhirType().equals(FHIRDefinedType.CODING)),
        "Input to display function must be Coding but is: " + inputPath.getExpression());

    final List<FhirPath> arguments = input.getArguments();
    checkUserInput(arguments.size() >= 0 && arguments.size() <= 1,
        NAME + " function accepts one optional language argument");
  }
}
