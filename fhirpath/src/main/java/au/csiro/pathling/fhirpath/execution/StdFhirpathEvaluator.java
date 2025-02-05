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

package au.csiro.pathling.fhirpath.execution;

import au.csiro.pathling.fhirpath.EvalOptions;
import au.csiro.pathling.fhirpath.EvaluationContext;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import au.csiro.pathling.fhirpath.context.FhirPathContext;
import au.csiro.pathling.fhirpath.context.ResourceResolver;
import au.csiro.pathling.fhirpath.context.ViewEvaluationContext;
import au.csiro.pathling.fhirpath.function.registry.FunctionRegistry;
import au.csiro.pathling.fhirpath.function.registry.StaticFunctionRegistry;
import au.csiro.pathling.fhirpath.variable.VariableResolverChain;
import jakarta.annotation.Nonnull;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Map;

@Value
@AllArgsConstructor(access = AccessLevel.PROTECTED)
@Builder
public class StdFhirpathEvaluator implements FhirpathEvaluator {

  // FOR: Javadocs
  public static class StdFhirpathEvaluatorBuilder {

  }
  
  @Nonnull
  ResourceResolver resourceResolver;

  @Nonnull
  @Builder.Default
  FunctionRegistry<?> functionRegistry = StaticFunctionRegistry.getInstance();

  @Nonnull
  @Builder.Default
  Map<String, Collection> variables = Map.of();

  @Nonnull
  @Builder.Default
  EvalOptions evalOptions = EvalOptions.getDefaults();

  @Nonnull
  public static StdFhirpathEvaluatorBuilder fromResolver(
      @Nonnull final ResourceResolver resourceResolver) {
    return StdFhirpathEvaluator.builder().resourceResolver(resourceResolver);
  }

  public StdFhirpathEvaluator(@Nonnull final ResourceResolver resourceResolver,
      @Nonnull final FunctionRegistry<?> functionRegistry,
      @Nonnull final Map<String, Collection> variables) {
    this(resourceResolver, functionRegistry, variables, EvalOptions.getDefaults());
  }

  @Nonnull
  @Override
  public Collection evaluate(@Nonnull final FhirPath path, @Nonnull final Collection inputContext) {
    final ResourceCollection resource = createDefaultInputContext();
    final VariableResolverChain variableResolverChain =
        VariableResolverChain.withDefaults(resource, inputContext, variables);
    final FhirPathContext fhirpathContext = FhirPathContext.of(
        resource, inputContext, variableResolverChain);
    final EvaluationContext evalContext = new ViewEvaluationContext(fhirpathContext,
        functionRegistry, resourceResolver, evalOptions);
    return path.apply(inputContext, evalContext);
  }

  @Nonnull
  @Override
  public ResourceCollection createDefaultInputContext() {
    return resourceResolver.resolveSubjectResource();
  }

  @Nonnull
  @Override
  public Dataset<Row> createInitialDataset() {
    return resourceResolver.createView();
  }

}
