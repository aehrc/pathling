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

import static au.csiro.pathling.library.FhirMimeTypes.FHIR_JSON;
import static au.csiro.pathling.library.FhirMimeTypes.FHIR_XML;

import au.csiro.pathling.encoders.FhirEncoders;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.parser.IParser;
import java.util.Iterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nonnull;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.hl7.fhir.instance.model.api.IBaseResource;

abstract class EncodeMapPartitions<T extends IBaseResource> implements
    MapPartitionsFunction<String, T> {

  private static final long serialVersionUID = -189338116652852324L;
  protected final FhirVersionEnum fhirVersion;
  protected final String inputMimeType;
  protected final Class<T> resourceClass;

  protected EncodeMapPartitions(FhirVersionEnum fhirVersion, final String inputMimeType,
      Class<T> resourceClass) {
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

  @Override
  @Nonnull
  public Iterator<T> call(@Nonnull final Iterator<String> iterator) {
    final IParser parser = createParser(inputMimeType);

    final Iterable<String> iterable = () -> iterator;
    final Stream<IBaseResource> parsedResources = StreamSupport.stream(iterable.spliterator(),
            false)
        .map(parser::parseResource);
    //noinspection unchecked
    return (Iterator<T>) processResources(parsedResources).iterator();
  }

  @Nonnull
  protected IParser createParser(@Nonnull final String mimeType) {
    final FhirContext fhirContext = FhirEncoders.contextFor(fhirVersion);
    switch (mimeType) {
      case FHIR_JSON:
        return fhirContext.newJsonParser();
      case FHIR_XML:
        return fhirContext.newXmlParser();
      default:
        throw new IllegalArgumentException("Cannot create FHIR parser for mime type: " + mimeType);
    }
  }
}
