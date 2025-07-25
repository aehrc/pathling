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

package au.csiro.pathling.fhirpath.context;

import static au.csiro.pathling.fhirpath.FhirPathConstants.PredefinedVariables.*;

import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import au.csiro.pathling.fhirpath.variable.EnvironmentVariableResolver;
import jakarta.annotation.Nonnull;
import lombok.Value;

@Value(staticConstructor = "of")
public class FhirPathContext {

  @Nonnull
  ResourceCollection resource;

  @Nonnull
  Collection inputContext;

  @Nonnull
  EnvironmentVariableResolver variables;

  @Nonnull
  public Collection resolveVariable(@Nonnull final String name) {
    if (name.equals(CONTEXT)) {
      return inputContext;
    } else if (name.equals(RESOURCE) || name.equals(ROOT_RESOURCE)) {
      return resource;
    } else {
      return variables.get(name)
          .orElseThrow(() -> new IllegalArgumentException("Unknown variable: " + name));
    }
  }
}
