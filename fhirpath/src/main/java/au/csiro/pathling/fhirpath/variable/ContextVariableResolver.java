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

package au.csiro.pathling.fhirpath.variable;

import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import jakarta.annotation.Nonnull;
import java.util.Optional;

/**
 * A resolver that provides access to the input context and resource.
 *
 * @param resource the resource collection representing the root resource
 * @param inputContext the collection representing the input context
 * @author John Grimes
 * @see <a href="https://hl7.org/fhirpath/#environment-variables">FHIRPath environment variables</a>
 * @see <a href="https://hl7.org/fhir/R4/fhirpath.html#variables">FHIR-specific environment
 *     variables</a>
 */
public record ContextVariableResolver(
    @Nonnull ResourceCollection resource, @Nonnull Collection inputContext)
    implements EnvironmentVariableResolver {

  @Override
  public Optional<Collection> get(@Nonnull final String name) {
    if (name.equals("context")) {
      return Optional.of(inputContext);
    } else if (name.equals("resource") || name.equals("rootResource")) {
      return Optional.of(resource);
    } else {
      return Optional.empty();
    }
  }
}
