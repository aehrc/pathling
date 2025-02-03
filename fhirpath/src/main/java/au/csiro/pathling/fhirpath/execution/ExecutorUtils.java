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

import static au.csiro.pathling.fhirpath.execution.DataRoot.ResourceRoot;
import static au.csiro.pathling.fhirpath.execution.DataRoot.ReverseResolveRoot;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.path.Paths.EvalFunction;
import au.csiro.pathling.fhirpath.path.Paths.Resource;
import jakarta.annotation.Nonnull;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

public class ExecutorUtils {

  @Nonnull
  public static ReverseResolveRoot fromPath(@Nonnull final DataRoot master,
      @Nonnull final EvalFunction reverseJoin) {
    final FhirPath reference = reverseJoin.getArguments().get(0);
    final Resource foreingResource = (Resource) reference.first();
    // TODO: check that te rest is a valid traveral only path
    final String foreignResourcePath = reference.suffix().toExpression();
    return ReverseResolveRoot.of(master, foreingResource.getResourceType(),
        foreignResourcePath);
  }

  @Nonnull
  public static ReverseResolveRoot fromPath(@Nonnull final ResourceType masterType,
      @Nonnull final EvalFunction reverseJoin) {
    return fromPath(ResourceRoot.of(masterType), reverseJoin);
  }

}
