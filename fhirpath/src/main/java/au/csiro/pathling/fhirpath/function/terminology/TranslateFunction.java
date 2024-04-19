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
import static au.csiro.pathling.fhirpath.TerminologyUtils.isCodeableConcept;
import static au.csiro.pathling.fhirpath.function.NamedFunction.expressionFromInput;
import static au.csiro.pathling.sql.Terminology.translate;
import static au.csiro.pathling.sql.TerminologySupport.parseCsvEquivalences;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.TerminologyUtils;
import au.csiro.pathling.fhirpath.element.ElementDefinition;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.function.Arguments;
import au.csiro.pathling.fhirpath.function.NamedFunction;
import au.csiro.pathling.fhirpath.function.NamedFunctionInput;
import au.csiro.pathling.fhirpath.literal.BooleanLiteralPath;
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
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.StringType;

/**
 * A function that takes a set of Codings or CodeableConcepts as inputs and returns a set Codings
 * translated using provided concept map URL.
 * <p>
 * Signature:
 * <pre>
 * collection&lt;Coding|CodeableConcept&gt; -&gt; translate(conceptMapUrl: string, reverse = false,
 * equivalence = 'equivalent') : collection&lt;Coding&gt;
 * </pre>
 * <p>
 * Uses: <a href="https://www.hl7.org/fhir/operation-conceptmap-translate.html">Translate
 * Operation</a>
 *
 * @author Piotr Szul
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#translate">translate</a>
 */
public class TranslateFunction implements NamedFunction {

  private static final String NAME = "translate";

  private static final Boolean DEFAULT_REVERSE = false;

  private static final String DEFAULT_EQUIVALENCE = "equivalent";


  @Nonnull
  @Override
  public FhirPath invoke(@Nonnull final NamedFunctionInput input) {
    validateInput(input);

    final ElementPath inputPath = (ElementPath) input.getInput();

    final Column idColumn = inputPath.getIdColumn();
    final boolean isCodeableConcept = isCodeableConcept(inputPath);
    // The definition of the result is always the Coding element.
    @SuppressWarnings("OptionalGetWithoutIsPresent")
    final ElementDefinition resultDefinition = isCodeableConcept
                                               ? inputPath.getChildElement("coding").get()
                                               : inputPath.getDefinition().get();

    final Arguments arguments = Arguments.of(input);
    final String conceptMapUrl = arguments.getValue(0, StringType.class).asStringValue();
    final boolean reverse = arguments.getValueOr(1, new BooleanType(DEFAULT_REVERSE))
        .booleanValue();
    final String equivalencesCsv = arguments.getValueOr(2, new StringType(DEFAULT_EQUIVALENCE))
        .asStringValue();
    @Nullable final String target = Optional.ofNullable(
            arguments.getNullableValue(3, StringType.class))
        .map(StringType::asStringValue)
        .orElse(null);
    final Dataset<Row> dataset = inputPath.getDataset();
    final Column translatedCodings = translate(getCodingColumn(inputPath), conceptMapUrl, reverse,
        parseCsvEquivalences(equivalencesCsv), target);

    // // The result is an array of translations per each input element, which we now
    // // need to explode in the same way as for path traversal, creating unique element ids.
    final MutablePair<Column, Column> valueAndEidColumns = new MutablePair<>();
    final Dataset<Row> resultDataset = inputPath
        .explodeArray(dataset, translatedCodings, valueAndEidColumns);

    final String expression = expressionFromInput(input, NAME);

    return ElementPath.build(expression, resultDataset, idColumn,
        Optional.of(valueAndEidColumns.getRight()), valueAndEidColumns.getLeft(), false,
        inputPath.getCurrentResource(), inputPath.getThisColumn(), resultDefinition);
  }

  private void validateInput(@Nonnull final NamedFunctionInput input) {
    final ParserContext context = input.getContext();
    checkUserInput(context.getTerminologyServiceFactory().isPresent(),
        "Attempt to call terminology function " + NAME
            + " when terminology service has not been configured");

    final FhirPath inputPath = input.getInput();
    checkUserInput(TerminologyUtils.isCodingOrCodeableConcept(inputPath),
        String.format("Input to %s function is of unsupported type: %s", NAME,
            inputPath.getExpression()));
    final List<FhirPath> arguments = input.getArguments();
    checkUserInput(arguments.size() >= 1 && arguments.size() <= 3,
        NAME + " function accepts one required and two optional arguments");
    checkUserInput(arguments.get(0) instanceof StringLiteralPath,
        String.format("Function `%s` expects `%s` as argument %s", NAME, "String literal", 1));
    checkUserInput(arguments.size() <= 1 || arguments.get(1) instanceof BooleanLiteralPath,
        String.format("Function `%s` expects `%s` as argument %s", NAME, "Boolean literal", 2));
    checkUserInput(arguments.size() <= 2 || arguments.get(2) instanceof StringLiteralPath,
        String.format("Function `%s` expects `%s` as argument %s", NAME, "String literal", 3));
  }

}
