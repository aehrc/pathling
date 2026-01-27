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

package au.csiro.pathling.library;

import ca.uhn.fhir.context.FhirVersionEnum;
import jakarta.annotation.Nonnull;
import java.io.Serial;
import java.util.Iterator;
import java.util.stream.StreamSupport;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.hl7.fhir.instance.model.api.IBaseResource;

/**
 * Encodes a partition of resources into strings, based upon a specified FHIR encoding type.
 *
 * @param <T> the type of resource to encode
 * @author John Grimes
 */
class DecodeResourceMapPartitions<T extends IBaseResource>
    implements MapPartitionsFunction<T, String> {

  @Serial private static final long serialVersionUID = -13742787546282853L;

  @Nonnull protected final FhirVersionEnum fhirVersion;

  @Nonnull protected final String outputMimeType;

  public DecodeResourceMapPartitions(
      @Nonnull final FhirVersionEnum fhirVersion, @Nonnull final String outputMimeType) {
    this.fhirVersion = fhirVersion;
    this.outputMimeType = outputMimeType;
  }

  @Override
  public Iterator<String> call(final Iterator<T> iterator) {
    final ResourceParser parser = ResourceParser.build(fhirVersion, outputMimeType);
    final Iterable<T> iterable = () -> iterator;
    return StreamSupport.stream(iterable.spliterator(), false).map(parser::encode).iterator();
  }
}
