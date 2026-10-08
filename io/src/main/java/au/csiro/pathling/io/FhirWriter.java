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

package au.csiro.pathling.io;

import au.csiro.pathling.definition.DefinitionContext;
import jakarta.annotation.Nonnull;

/**
 * Writes resources stored in this layout as FHIR JSON or XML (decision 84).
 *
 * <p>This is where every write begins. A writer for one format is obtained by name, {@link #json()}
 * or {@link #xml()}, or by media type, {@link #format(String)}:
 *
 * <pre>{@code
 * FhirWriter writer = FhirWriter.of(definitions);
 * writer.json().write("Patient", stored, "/data/Patient", SaveMode.ErrorIfExists);
 * writer.xml().write("Patient", stored);
 * }</pre>
 *
 * <p>How each format is written is not part of this API, and is free to change.
 */
public final class FhirWriter {

  @Nonnull private final FhirJsonWriter json;

  @Nonnull private final FhirXmlWriter xml;

  private FhirWriter(@Nonnull final FhirJsonWriter json, @Nonnull final FhirParsers parsers) {
    this.json = json;
    this.xml = new FhirXmlWriter(json, parsers);
  }

  /**
   * Returns a writer of resources stored as the definitions describe.
   *
   * @param definitions the definitions the resources were stored with
   * @return the writer
   */
  @Nonnull
  public static FhirWriter of(@Nonnull final DefinitionContext definitions) {
    return new FhirWriter(
        new FhirJsonWriter(ResourceTransformer.of(definitions)), FhirParsers.of(definitions));
  }

  /**
   * Returns the writer of FHIR JSON.
   *
   * @return the writer
   */
  @Nonnull
  public FhirJsonWriter json() {
    return json;
  }

  /**
   * Returns the writer of FHIR XML.
   *
   * @return the writer
   */
  @Nonnull
  public FhirXmlWriter xml() {
    return xml;
  }

  /**
   * Returns the writer of the format a media type names, which is {@code application/fhir+json} or
   * {@code application/fhir+xml}.
   *
   * @param mediaType the media type
   * @return the writer
   * @throws IllegalArgumentException if the media type names neither format
   */
  @Nonnull
  public FhirFormatWriter format(@Nonnull final String mediaType) {
    return FhirMediaType.select(mediaType, json, xml);
  }
}
