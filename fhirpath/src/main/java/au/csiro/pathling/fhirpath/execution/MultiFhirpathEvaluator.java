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

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.function.registry.FunctionRegistry;
import au.csiro.pathling.io.source.DataSource;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import lombok.Value;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;


/**
 * A FHIRPath evaluator that can handle multiple joins.
 */
@Slf4j
@UtilityClass
public class MultiFhirpathEvaluator {


  @Value(staticConstructor = "of")
  public static class ManyFactory implements FhirpathEvaluator.Factory {

    @Nonnull
    ResourceType subjectResource;

    @Nonnull
    FhirContext fhirContext;

    @Nonnull
    DataSource dataSource;

    @Nonnull
    JoinSet subjectJoinSet;

    @Override
    @Nonnull
    public FhirpathEvaluator create(
        @Nonnull final ResourceType subjectResource,
        @Nonnull final FunctionRegistry<?> functionRegistry,
        @Nonnull final Map<String, Collection> variables) {

      if (!subjectResource.equals(this.subjectResource)) {
        throw new IllegalArgumentException(
            "subjectResource must be the same as the one used to create the factory");
      }
      return new StdFhirpathEvaluator(
          new ManyResourceResolver(subjectResource, fhirContext, dataSource, subjectJoinSet),
          functionRegistry, variables);
    }

    @Nonnull
    public static ManyFactory fromPaths(@Nonnull ResourceType subjectResource,
        @Nonnull final FhirContext fhirContext,
        @Nonnull final DataSource dataSource,
        @Nonnull final List<FhirPath> contextPaths) {

      final JoinSet joinSet = fromContextPaths(
          subjectResource, fhirContext, contextPaths
      );
      return of(
          subjectResource,
          fhirContext, dataSource, joinSet
      );
    }
  }

  @Value
  public static class ManyProvider implements FhirpathEvaluator.Provider {

    @Nonnull
    FhirContext fhirContext;

    @Nonnull
    FunctionRegistry<?> functionRegistry;

    @Nonnull
    Map<String, Collection> variables;

    @Nonnull
    DataSource dataSource;

    @Nonnull
    @Override
    public FhirpathEvaluator create(@Nonnull final ResourceType subjectResource,
        @Nonnull final Supplier<List<FhirPath>> contextPathsSupplier) {
      final List<FhirPath> contextPaths = contextPathsSupplier.get();
      final JoinSet subjectJoinSet = fromContextPaths(subjectResource, fhirContext, contextPaths);
      return new StdFhirpathEvaluator(
          new ManyResourceResolver(subjectResource, fhirContext, dataSource, subjectJoinSet),
          functionRegistry, variables);
    }
  }

  // TODO: Determine at which stage we actually need the subjectResource
  @Nonnull
  public static JoinSet fromContextPaths(@Nonnull final ResourceType subjectResource,
      @Nonnull final FhirContext fhirContext,
      @Nonnull final List<FhirPath> contextPaths) {
    final DataRootResolver dataRootResolver = new DataRootResolver(subjectResource, fhirContext);
    final Set<DataRoot> joinRoots = dataRootResolver.findDataRoots(contextPaths);
    log.debug("Join roots: {}", joinRoots);
    final JoinSet joinSet = JoinSet.mergeRoots(joinRoots).iterator().next();
    log.debug("Join set:\n{}", joinSet.toTreeString());
    return joinSet;
  }
}
