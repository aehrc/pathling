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

import static au.csiro.pathling.fhirpath.TerminologyUtils.getCodingColumn;
import static au.csiro.pathling.fhirpath.function.NamedFunction.expressionFromInput;
import static au.csiro.pathling.sql.Terminology.member_of;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;

import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.PrimitivePath;
import au.csiro.pathling.fhirpath.function.NamedFunction;
import au.csiro.pathling.fhirpath.literal.StringLiteralPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * A function that takes a set of Codings or CodeableConcepts as inputs and returns a set of boolean
 * values, based upon whether each item is present within the ValueSet identified by the supplied
 * URL.
 *
 * @author John Grimes
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#memberof">memberOf</a>
 */
@Slf4j
public class MemberOfFunction implements NamedFunction {

  private static final String NAME = "memberOf";

  /**
   * Returns a new instance of this function.
   */
  public MemberOfFunction() {
  }

  @Nonnull
  @Override
  public Collection invoke(@Nonnull final NamedFunctionInput input) {
    validateInput(input);
    final PrimitivePath inputPath = (PrimitivePath) input.getInput();
    final StringLiteralPath argument = (StringLiteralPath) input.getArguments().get(0);
    final String valueSetUrl = argument.getValue().asStringValue();

    final Column resultColumn = member_of(getCodingColumn(inputPath), valueSetUrl);
    // Construct a new result expression.
    final String expression = expressionFromInput(input, NAME, input.getInput());

    return PrimitivePath
        .build(expression, inputPath.getDataset(), inputPath.getIdColumn(), resultColumn,
            inputPath.getOrderingColumn(), inputPath.isSingular(), inputPath.getCurrentResource(),
            inputPath.getThisColumn(), FHIRDefinedType.BOOLEAN);
  }

  private void validateInput(@Nonnull final NamedFunctionInput input) {
    final ParserContext context = input.getContext();
    checkUserInput(context.getTerminologyServiceFactory()
        .isPresent(), "Attempt to call terminology function " + NAME
        + " when terminology service has not been configured");

    final Collection inputPath = input.getInput();
    checkUserInput(inputPath instanceof PrimitivePath
            && (((PrimitivePath) inputPath).getFhirType().equals(FHIRDefinedType.CODING)
            || ((PrimitivePath) inputPath).getFhirType().equals(FHIRDefinedType.CODEABLECONCEPT)),
        "Input to memberOf function is of unsupported type: " + inputPath.getExpression());
    checkUserInput(input.getArguments().size() == 1,
        "memberOf function accepts one argument of type String");
    final Collection argument = input.getArguments().get(0);
    checkUserInput(argument instanceof StringLiteralPath,
        "memberOf function accepts one argument of type String literal");
  }

}
