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

package au.csiro.pathling.library;

import ca.uhn.fhir.context.FhirVersionEnum;
import jakarta.annotation.Nonnull;
import java.io.Serial;
import java.util.stream.Stream;
import org.hl7.fhir.instance.model.api.IBaseResource;

class EncodeResourceMapPartitions<T extends IBaseResource> extends EncodeMapPartitions<T> {

  @Serial private static final long serialVersionUID = 6405663463302424287L;

  EncodeResourceMapPartitions(
      final FhirVersionEnum fhirVersion, final String inputMimeType, final Class<T> resourceClass) {
    super(fhirVersion, inputMimeType, resourceClass);
  }

  @Nonnull
  @Override
  protected Stream<IBaseResource> processResources(@Nonnull final Stream<IBaseResource> resources) {
    return resources.filter(resourceClass::isInstance);
  }
}
