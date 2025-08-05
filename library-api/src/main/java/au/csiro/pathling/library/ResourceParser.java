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

import static au.csiro.pathling.library.PathlingContext.FHIR_JSON;
import static au.csiro.pathling.library.PathlingContext.FHIR_XML;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.support.FhirConversionSupport;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.parser.IParser;
import jakarta.annotation.Nonnull;
import org.hl7.fhir.instance.model.api.IBaseBundle;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Bundle;

/**
 * A wrapper around a FHIR parser that provides additional functionality compensating for the issues
 * with HAPI {@link IParser} implementations. In particular, this class ensures that ids are
 * preserved and URN references are resolved when parsing a {@link Bundle}.
 */
class ResourceParser {

  @Nonnull
  private final IParser parser;

  @Nonnull
  private final FhirConversionSupport conversionSupport;

  private ResourceParser(@Nonnull final IParser parser,
      @Nonnull final FhirConversionSupport conversionSupport) {
    this.parser = parser;
    this.conversionSupport = conversionSupport;
    // setup parser
    this.parser.setPrettyPrint(false);
    this.parser.setOverrideResourceIdWithBundleEntryFullUrl(false);
  }


  /**
   * Parses the given resource string into a FHIR resource.
   *
   * @param resourceString the string representation of the resource
   * @return the parsed resource
   */
  @Nonnull
  public IBaseResource parse(@Nonnull final String resourceString) {
    final IBaseResource resource = parser.parseResource(resourceString);
    return (resource instanceof final IBaseBundle bundle)
           ? conversionSupport.resolveReferences(bundle)
           : resource;
  }

  /**
   * Encodes the given FHIR resource into a string.
   *
   * @param resource the resource to encode
   * @return the string representation of the resource
   */
  @Nonnull
  public String encode(@Nonnull final IBaseResource resource) {
    return parser.encodeResourceToString(resource);
  }

  /**
   * Creates a new FHIR parser for the given FHIR version and mime type.
   *
   * @param fhirVersion the FHIR version
   * @param mimeType the mime type
   * @return a new FHIR parser
   */
  @Nonnull
  public static ResourceParser build(@Nonnull final FhirVersionEnum fhirVersion,
      @Nonnull final String mimeType) {
    final FhirContext fhirContext = FhirEncoders.contextFor(fhirVersion);
    final FhirConversionSupport conversionSupport = FhirConversionSupport.supportFor(fhirVersion);
    return switch (mimeType) {
      case FHIR_JSON -> new ResourceParser(fhirContext.newJsonParser(), conversionSupport);
      case FHIR_XML -> new ResourceParser(fhirContext.newXmlParser(), conversionSupport);
      default -> throw new IllegalArgumentException(
          "Cannot create FHIR parser for mime type: " + mimeType);
    };
  }
}
