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
import javax.annotation.Nonnull;

public class ParserBuilder {

  public static IParser build(@Nonnull final FhirVersionEnum fhirVersion,
      @Nonnull final String mimeType) {
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
