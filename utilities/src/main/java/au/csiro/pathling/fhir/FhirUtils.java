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

package au.csiro.pathling.fhir;

import jakarta.annotation.Nullable;
import org.hl7.fhir.exceptions.FHIRException;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * A utility class for working with FHIR resources.
 *
 * @author John Grimes
 */
public class FhirUtils {

  /**
   * A method for getting a FHIR resource type from a string. This method will always throw an error
   * if the string does not match a known resource type.
   *
   * @param resourceCode the string to convert to a resource type
   * @return the resource type
   */
  public static ResourceType getResourceType(@Nullable final String resourceCode) {
    @Nullable final ResourceType resourceType = ResourceType.fromCode(resourceCode);
    if (resourceType == null) {
      throw new FHIRException("Unknown resource type: " + resourceCode);
    }
    return resourceType;
  }

}
