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

package au.csiro.pathling.library;

import ca.uhn.fhir.context.FhirVersionEnum;
import jakarta.annotation.Nonnull;
import java.util.Iterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.hl7.fhir.instance.model.api.IBaseResource;

abstract class EncodeMapPartitions<T extends IBaseResource> implements
    MapPartitionsFunction<String, T> {

  private static final long serialVersionUID = -189338116652852324L;

  @Nonnull
  protected final FhirVersionEnum fhirVersion;

  @Nonnull
  protected final String inputMimeType;

  @Nonnull
  protected final Class<T> resourceClass;

  protected EncodeMapPartitions(@Nonnull final FhirVersionEnum fhirVersion,
      @Nonnull final String inputMimeType, @Nonnull final Class<T> resourceClass) {
    this.fhirVersion = fhirVersion;
    this.inputMimeType = inputMimeType;
    this.resourceClass = resourceClass;
  }

  /**
   * Converts the parsed input resouces to the final stream of requested resources. May include
   * filtering, extracting from bundles etc.
   *
   * @param resources the stream of all input resources
   * @return the stream of desired resources
   */
  @Nonnull
  protected abstract Stream<IBaseResource> processResources(
      @Nonnull final Stream<IBaseResource> resources);

  @SuppressWarnings("unchecked")
  @Override
  @Nonnull
  public Iterator<T> call(@Nonnull final Iterator<String> iterator) {
    final ResourceParser parser = ResourceParser.build(fhirVersion, inputMimeType);

    final Iterable<String> iterable = () -> iterator;
    final Stream<IBaseResource> parsedResources = StreamSupport.stream(iterable.spliterator(),
            false)
        .map(parser::parse);
    return (Iterator<T>) processResources(parsedResources).iterator();
  }
}
