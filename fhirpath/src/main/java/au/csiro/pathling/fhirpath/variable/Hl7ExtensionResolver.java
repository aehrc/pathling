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

package au.csiro.pathling.fhirpath.variable;

import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.StringCollection;
import jakarta.annotation.Nonnull;
import java.util.Optional;

/**
 * A resolver for HL7 FHIR Extensions.
 *
 * @author John Grimes
 * @see <a href="https://build.fhir.org/fhirpath.html#vars">FHIR-specific environment variables</a>
 */
public class Hl7ExtensionResolver implements EnvironmentVariableResolver {

  private static final String NAME_PREFIX = "ext-";

  @Override
  public Optional<Collection> get(@Nonnull final String name) {
    if (name.startsWith(NAME_PREFIX)) {
      final String extensionName = name.substring(NAME_PREFIX.length());
      return Optional.of(
          StringCollection.fromValue("http://hl7.org/fhir/StructureDefinition/" + extensionName));
    } else {
      return Optional.empty();
    }
  }
}
