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

import au.csiro.pathling.fhirpath.annotations.Name;
import au.csiro.pathling.fhirpath.annotations.NotImplemented;

/**
 * A function for resolving a Reference element in order to access the elements of the target
 * resource. Supports polymorphic references through the use of an argument specifying the target
 * resource type.
 *
 * @author John Grimes
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#resolve">resolve</a>
 */
@Name("resolve")
@NotImplemented
public class ResolveFunction implements NamedFunction {

  // TODO: implement with columns
  // private static final String NAME = "resolve";
  //
  // public ResolveFunction() {
  // }
  //
  // @Nonnull
  // @Override
  // public Collection invoke(@Nonnull final NamedFunctionInput input) {
  //   checkUserInput(input.getInput() instanceof ReferencePath,
  //       "Input to " + NAME + " function must be a Reference: " + input.getInput().getExpression());
  //   checkNoArguments(NAME, input);
  //   final ReferencePath inputPath = (ReferencePath) input.getInput();
  //   final DataSource dataSource = input.getContext().getDataSource();
  //
  //   // Get the allowed types for the input reference. This gives us the set of possible resource
  //   // types that this reference could resolve to.
  //   Set<ResourceType> referenceTypes = inputPath.getResourceTypes();
  //   // If the type is Resource, all resource types need to be looked at.
  //   if (referenceTypes.contains(ResourceType.RESOURCE)) {
  //     referenceTypes = ResourceCollection.supportedResourceTypes();
  //   }
  //   check(referenceTypes.size() > 0);
  //   final boolean isPolymorphic = referenceTypes.size() > 1;
  //
  //   final String expression = expressionFromInput(input, NAME, input.getInput());
  //
  //   if (isPolymorphic) {
  //     final ReferencePath referencePath = (ReferencePath) input.getInput();
  //     return UntypedResourcePath.build(referencePath, expression);
  //   } else {
  //     final FhirContext fhirContext = input.getContext().getFhirContext();
  //     final ResourceType resourceType = (ResourceType) referenceTypes.toArray()[0];
  //     return resolveMonomorphicReference(inputPath, dataSource, fhirContext, resourceType,
  //         expression, input.getContext());
  //   }
  // }
  //
  // @Nonnull
  // public static Collection resolveMonomorphicReference(@Nonnull final ReferencePath referencePath,
  //     @Nonnull final DataSource dataSource, @Nonnull final FhirContext fhirContext,
  //     @Nonnull final ResourceType resourceType, @Nonnull final String expression,
  //     @Nonnull final ParserContext context) {
  //   // If this is a monomorphic reference, we just need to retrieve the appropriate table and
  //   // create a dataset with the full resources.
  //   final ResourceCollection resourceCollection = ResourceCollection.build(fhirContext, dataSource,
  //       resourceType,
  //       expression, referencePath.isSingular());
  //   final BasicElementDefinition referenceDefinition = checkPresent(referencePath.getDefinition());
  //   final ReferenceNestingKey referenceNestingKey = new ReferenceNestingKey(referenceDefinition,
  //       resourceCollection.getDefinition());
  //
  //   return context.getNesting()
  //       .updateOrRetrieve(referenceNestingKey, expression, referencePath.getDataset(),
  //           referencePath.isSingular(), referencePath.getThisColumn(), key -> {
  //             // Join the resource dataset to the reference dataset.
  //             final Column joinCondition = referencePath.getResourceEquality(resourceCollection);
  //             final Dataset<Row> dataset = join(referencePath.getDataset(),
  //                 resourceCollection.getDataset(),
  //                 joinCondition, JoinType.LEFT_OUTER);
  //
  //             final Column inputId = referencePath.getIdColumn();
  //             final ResourceCollection result = resourceCollection.copy(expression, dataset,
  //                 inputId,
  //                 resourceCollection.getValueColumn(), resourceCollection.getOrderingColumn(),
  //                 referencePath.isSingular(), referencePath.getThisColumn());
  //             result.setCurrentResource(resourceCollection);
  //
  //             return result;
  //           });
  // }

}
