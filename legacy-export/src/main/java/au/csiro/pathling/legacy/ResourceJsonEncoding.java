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
package au.csiro.pathling.legacy;

import au.csiro.pathling.encoders.FhirEncoders;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.parser.IParser;
import jakarta.annotation.Nonnull;
import java.io.Serial;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.hl7.fhir.instance.model.api.IBaseResource;

/**
 * Serialises a partition of decoded resources as FHIR JSON, one document per resource, each on a
 * single line.
 *
 * @author Piotr Szul
 */
final class ResourceJsonEncoding implements MapPartitionsFunction<IBaseResource, String> {

  @Serial private static final long serialVersionUID = 2939410163262651379L;

  @Override
  @Nonnull
  public Iterator<String> call(@Nonnull final Iterator<IBaseResource> resources) {
    // The context is cached by the encoders and is not serialisable, so it is obtained here, on the
    // executor, rather than carried by this function.
    final IParser parser =
        FhirEncoders.contextFor(FhirVersionEnum.R4).newJsonParser().setPrettyPrint(false);
    return StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(resources, Spliterator.ORDERED), false)
        .map(parser::encodeResourceToString)
        .iterator();
  }
}
