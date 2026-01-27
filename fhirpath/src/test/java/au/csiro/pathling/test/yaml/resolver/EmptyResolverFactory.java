/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.test.yaml.resolver;

import au.csiro.pathling.fhirpath.definition.defaults.DefaultDefinitionContext;
import au.csiro.pathling.fhirpath.definition.defaults.DefaultResourceDefinition;
import au.csiro.pathling.fhirpath.definition.defaults.DefaultResourceTag;
import au.csiro.pathling.fhirpath.evaluation.DatasetEvaluator;
import au.csiro.pathling.fhirpath.evaluation.DefinitionResourceResolver;
import au.csiro.pathling.fhirpath.evaluation.ResourceResolver;
import au.csiro.pathling.fhirpath.evaluation.SingleResourceEvaluator;
import au.csiro.pathling.fhirpath.function.registry.StaticFunctionRegistry;
import java.util.Map;
import java.util.function.Function;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Value;

/**
 * Factory for creating empty DatasetEvaluator instances. This implementation provides an evaluator
 * with an empty DataFrame, useful for testing expressions that don't require input data.
 */
@Value
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class EmptyResolverFactory implements Function<RuntimeContext, DatasetEvaluator> {

  // singleton
  private static final EmptyResolverFactory INSTANCE = new EmptyResolverFactory();

  public static EmptyResolverFactory getInstance() {
    return INSTANCE;
  }

  @Override
  public DatasetEvaluator apply(final RuntimeContext runtimeContext) {
    final String resourceCode = "Empty";
    final DefaultResourceTag subjectResourceTag = DefaultResourceTag.of(resourceCode);
    final DefaultResourceDefinition resourceDefinition =
        DefaultResourceDefinition.of(subjectResourceTag);

    // Create resolver using DefinitionResourceResolver
    final ResourceResolver resolver =
        DefinitionResourceResolver.of(
            resourceCode, DefaultDefinitionContext.of(resourceDefinition));

    // Create the evaluator with the resolver
    final SingleResourceEvaluator evaluator =
        SingleResourceEvaluator.of(resolver, StaticFunctionRegistry.getInstance(), Map.of());

    // Create and return DatasetEvaluator with empty dataset
    return new DatasetEvaluator(evaluator, runtimeContext.getSpark().emptyDataFrame());
  }
}
