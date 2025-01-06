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
import org.hl7.fhir.r4.model.Enumerations.ResourceType;


/**
 * A FHIRPath evaluator that can handle multiple joins.
 */
@UtilityClass
public class MultiFhirpathEvaluator {

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
      final DataRootResolver dataRootResolver = new DataRootResolver(subjectResource, fhirContext);
      final Set<DataRoot> joinRoots = dataRootResolver.findDataRoots(contextPaths);
      System.out.println("Join roots: " + joinRoots);
      final JoinSet subjectJoinSet = JoinSet.mergeRoots(joinRoots).iterator().next();
      System.out.println("Subject join set: \n" + subjectJoinSet.toTreeString());

      return new StdFhirpathEvaluator(
          new ManyResourceResolver(subjectResource, fhirContext, dataSource, subjectJoinSet),
          functionRegistry, variables);
    }
  }
}
