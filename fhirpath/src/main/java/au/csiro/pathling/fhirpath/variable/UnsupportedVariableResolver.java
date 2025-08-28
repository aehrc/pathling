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

import au.csiro.pathling.errors.UnsupportedFhirPathFeatureError;
import au.csiro.pathling.fhirpath.collection.Collection;
import jakarta.annotation.Nonnull;
import java.util.Optional;

/**
 * This resolver throws errors for known unsupported environment variables in FHIRPath.
 *
 * @author John Grimes
 */
public class UnsupportedVariableResolver implements EnvironmentVariableResolver {

  @Override
  public Optional<Collection> get(@Nonnull final String name) {
    return switch (name) {
      case "factory" -> throw new UnsupportedFhirPathFeatureError("Type factory is not supported");
      case "terminologies" ->
          throw new UnsupportedOperationException("Terminology service is not supported");
      case "server" ->
          throw new UnsupportedFhirPathFeatureError("General Service API is not supported");
      default -> Optional.empty();
    };
  }

}
