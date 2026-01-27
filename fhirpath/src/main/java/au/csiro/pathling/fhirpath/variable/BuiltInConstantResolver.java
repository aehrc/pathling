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
import au.csiro.pathling.fhirpath.collection.StringCollection;
import jakarta.annotation.Nonnull;
import java.util.Map;
import java.util.Optional;

/**
 * A resolver for built-in string constants.
 *
 * @author John Grimes
 * @see <a href="https://hl7.org/fhirpath/#environment-variables">FHIRPath environment variables</a>
 * @see <a href="https://hl7.org/fhir/R4/fhirpath.html#variables">FHIR-specific environment
 *     variables</a>
 */
public class BuiltInConstantResolver implements EnvironmentVariableResolver {

  private final Map<String, String> builtInVariables =
      Map.of(
          "ucum", "http://unitsofmeasure.org",
          "loinc", "http://loinc.org",
          "sct", "http://snomed.info/sct");

  @Override
  public Optional<Collection> get(@Nonnull final String name) {
    return Optional.ofNullable(builtInVariables.get(name)).map(StringCollection::fromValue);
  }
}
