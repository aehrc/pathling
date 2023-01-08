/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright 2023 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.support.r4;

import au.csiro.pathling.support.FhirConversionSupport;
import java.util.List;
import java.util.stream.Collectors;
import org.hl7.fhir.instance.model.api.IBase;
import org.hl7.fhir.instance.model.api.IBaseBundle;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;

public class R4FhirConversionSupport extends FhirConversionSupport {

  private static final long serialVersionUID = -367070946615790595L;

  @Override
  public String fhirType(IBase base) {

    return base.fhirType();
  }

  @Override
  public <T extends IBaseResource> List<IBaseResource> extractEntryFromBundle(IBaseBundle bundle,
      Class<T> resourceClass) {
    Bundle r4Bundle = (Bundle) bundle;

    return r4Bundle.getEntry().stream()
        .map(BundleEntryComponent::getResource)
        .filter(resourceClass::isInstance)
        .collect(Collectors.toList());
  }
}
