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

import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import lombok.Value;
import javax.annotation.Nonnull;

@Value(staticConstructor = "of")
public class FhirpathContext {

  @Nonnull
  ResourceCollection resource;

  @Nonnull
  Collection inputContext;

  @Nonnull
  public Collection resolveVariable(@Nonnull final String name) {
    if (name.equals("%context")) {
      return getInputContext();
    } else if (name.equals("%resource") || name.equals("%rootResource")) {
      return getResource();
    } else {
      throw new IllegalArgumentException("Unknown constant: " + name);
    }
  }
  
  public static FhirpathContext ofResource(@Nonnull final ResourceCollection resource) {
    return of(resource, resource);
  }
}