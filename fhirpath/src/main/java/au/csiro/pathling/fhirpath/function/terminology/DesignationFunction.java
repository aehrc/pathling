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

package au.csiro.pathling.fhirpath.function.terminology;

import static au.csiro.pathling.fhirpath.function.NamedFunction.expressionFromInput;
import static au.csiro.pathling.sql.Terminology.designation;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.function.Arguments;
import au.csiro.pathling.fhirpath.function.NamedFunction;
import au.csiro.pathling.fhirpath.function.NamedFunctionInput;
import au.csiro.pathling.fhirpath.literal.CodingLiteralPath;
import au.csiro.pathling.fhirpath.literal.StringLiteralPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.StringType;

/**
 * This function returns the designations of a Coding.
 *
 * @author Piotr Szul
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#designation">designation</a>
 */
public class DesignationFunction implements NamedFunction {

  private static final String NAME = "designation";

  @Nonnull
  @Override
  public FhirPath invoke(@Nonnull final NamedFunctionInput input) {

    validateInput(input);
    final ElementPath inputPath = (ElementPath) input.getInput();
    final String expression = expressionFromInput(input, NAME);

    final Arguments arguments = Arguments.of(input);
    @Nullable final Coding use = arguments.getOptionalValue(0, Coding.class).orElse(null);
    @Nullable final String languageCode = arguments.getOptionalValue(1, StringType.class)
        .map(StringType::getValue)
        .orElse(null);

    final Dataset<Row> dataset = inputPath.getDataset();
    final Column designations = designation(inputPath.getValueColumn(), use, languageCode);

    // // The result is an array of designations per each input element, which we now
    // // need to explode in the same way as for path traversal, creating unique element ids.
    final MutablePair<Column, Column> valueAndEidColumns = new MutablePair<>();
    final Dataset<Row> resultDataset = inputPath
        .explodeArray(dataset, designations, valueAndEidColumns);
    return ElementPath.build(expression, resultDataset, inputPath.getIdColumn(),
        Optional.of(valueAndEidColumns.getRight()),
        valueAndEidColumns.getLeft(), inputPath.isSingular(), inputPath.getCurrentResource(),
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
        "Input to " + NAME + " function must be Coding but is: " + inputPath.getExpression());

    final List<FhirPath> arguments = input.getArguments();
    checkUserInput(arguments.size() <= 2,
        NAME + " function accepts two optional arguments");
    checkUserInput(arguments.isEmpty() || arguments.get(0) instanceof CodingLiteralPath,
        String.format("Function `%s` expects `%s` as argument %s", NAME, "Coding literal", 1));
    checkUserInput(arguments.size() <= 1 || arguments.get(1) instanceof StringLiteralPath,
        String.format("Function `%s` expects `%s` as argument %s", NAME, "String literal", 2));
  }
}
