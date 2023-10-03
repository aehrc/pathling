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

import au.csiro.pathling.fhirpath.annotations.Name;
import au.csiro.pathling.fhirpath.annotations.NotImplemented;
import au.csiro.pathling.fhirpath.function.NamedFunction;

/**
 * This function returns the value of a property for a Coding.
 *
 * @author Piotr Szul
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#property">property</a>
 */
@Name("property")
@NotImplemented
public class PropertyFunction implements NamedFunction {

  // TODO: implement with columns

  // private static final String NAME = "property";
  //
  // @Nonnull
  // @Override
  // public Collection invoke(@Nonnull final NamedFunctionInput input) {
  //
  //   validateInput(input);
  //   final PrimitivePath inputPath = (PrimitivePath) input.getInput();
  //   final String expression = expressionFromInput(input, NAME, input.getInput());
  //
  //   final Arguments arguments = Arguments.of(input);
  //   final String propertyCode = arguments.getValue(0, StringType.class).asStringValue();
  //   final String propertyTypeAsString = arguments.getValueOr(1,
  //       new StringType(PropertyUdf.DEFAULT_PROPERTY_TYPE.toCode())).asStringValue();
  //   final Optional<StringType> preferredLanguage = arguments.getOptionalValue(2, StringType.class);
  //
  //   final FHIRDefinedType propertyType = wrapInUserInputError(FHIRDefinedType::fromCode).apply(
  //       propertyTypeAsString);
  //
  //   final Dataset<Row> dataset = inputPath.getDataset();
  //   final Column propertyValues = property_of(inputPath.getValueColumn(), propertyCode,
  //       propertyType, preferredLanguage.map(StringType::getValue).orElse(null));
  //
  //   // // The result is an array of property values per each input element, which we now
  //   // // need to explode in the same way as for path traversal, creating unique element ids.
  //   final MutablePair<Column, Column> valueAndOrderingColumns = new MutablePair<>();
  //   final Dataset<Row> resultDataset = explodeArray(dataset, propertyValues,
  //       valueAndOrderingColumns);
  //
  //   if (FHIRDefinedType.CODING.equals(propertyType)) {
  //     // Special case for CODING properties: we use the Coding definition form 
  //     // the input path so that the results can be further traversed.
  //     return inputPath.copy(expression, resultDataset, inputPath.getIdColumn(),
  //         valueAndOrderingColumns.getLeft(), Optional.of(valueAndOrderingColumns.getRight()),
  //         inputPath.isSingular(), inputPath.getThisColumn());
  //   } else {
  //     return PrimitivePath.build(expression, resultDataset, inputPath.getIdColumn(),
  //         valueAndOrderingColumns.getLeft(), Optional.of(valueAndOrderingColumns.getRight()),
  //         inputPath.isSingular(), inputPath.getCurrentResource(), inputPath.getThisColumn(),
  //         propertyType);
  //   }
  // }
  //
  // private void validateInput(@Nonnull final NamedFunctionInput input) {
  //   final ParserContext context = input.getContext();
  //   checkUserInput(context.getTerminologyServiceFactory()
  //       .isPresent(), "Attempt to call terminology function " + NAME
  //       + " when terminology service has not been configured");
  //
  //   final Collection inputPath = input.getInput();
  //   checkUserInput(inputPath instanceof PrimitivePath
  //           && (((PrimitivePath) inputPath).getFhirType().equals(FHIRDefinedType.CODING)),
  //       "Input to property function must be Coding but is: " + inputPath.getExpression());
  //
  //   final List<Collection> arguments = input.getArguments();
  //   checkUserInput(arguments.size() >= 1 && arguments.size() <= 3,
  //       NAME + " function accepts one required and one optional arguments");
  //   checkUserInput(arguments.get(0) instanceof StringLiteralPath,
  //       String.format("Function `%s` expects `%s` as argument %s", NAME, "String literal", 1));
  //   checkUserInput(arguments.size() <= 1 || arguments.get(1) instanceof StringLiteralPath,
  //       String.format("Function `%s` expects `%s` as argument %s", NAME, "String literal", 2));
  //   checkUserInput(arguments.size() <= 2 || arguments.get(2) instanceof StringLiteralPath,
  //       String.format("Function `%s` expects `%s` as argument %s", NAME, "String literal", 3));
  // }
}
