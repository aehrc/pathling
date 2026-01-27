/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.fhirpath.operator;

import au.csiro.pathling.fhirpath.EvaluationContext;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.TypeSpecifier;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.path.ParserPaths.TypeSpecifierPath;
import jakarta.annotation.Nonnull;

/**
 * Operator for the FHIRPath 'as' type-casting operator.
 *
 * <p>Returns the input collection if it contains a single item of the specified type, or an empty
 * collection otherwise. Throws an error if the input contains more than one item.
 *
 * <p>This operator provides the keyword syntax {@code value as Type}, complementing the function
 * syntax {@code value.as(Type)}.
 *
 * @author Piotr Szul
 * @see <a href="https://hl7.org/fhirpath/#astype--type-specifier">FHIRPath as operator</a>
 */
public class AsOperator implements FhirPathBinaryOperator {

  @Nonnull
  @Override
  public Collection invokeWithPaths(
      @Nonnull final EvaluationContext context,
      @Nonnull final Collection input,
      @Nonnull final FhirPath leftPath,
      @Nonnull final FhirPath rightPath) {
    // Evaluate the left operand (the value to cast)
    final Collection leftValue = leftPath.apply(input, context);

    // Extract TypeSpecifier from the right path (TypeSpecifierPath)
    // No need to evaluate - it's metadata, not a collection
    final TypeSpecifierPath typeSpecifierPath = (TypeSpecifierPath) rightPath;
    final TypeSpecifier typeSpecifier = typeSpecifierPath.getValue();

    // Delegate to Collection.asType() which handles the type casting
    // This returns the value if it matches the type, or empty otherwise
    return leftValue.asType(typeSpecifier);
  }

  @Nonnull
  @Override
  public Collection invoke(@Nonnull final BinaryOperatorInput input) {
    // This method won't be called for type operators since we override invokeWithPaths
    throw new UnsupportedOperationException("AsOperator should be invoked via invokeWithPaths()");
  }

  @Nonnull
  @Override
  public String getOperatorName() {
    return "as";
  }
}
