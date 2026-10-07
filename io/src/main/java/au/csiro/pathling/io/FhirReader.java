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
import org.apache.spark.sql.SparkSession;

/**
 * Reads FHIR resources into this layout, as JSON or XML, as resources or as the bundles that carry
 * them (decision 84).
 *
 * <p>This is where every read begins. A reader for one format is obtained by name, {@link #json()}
 * or {@link #xml()}, or by media type, {@link #format(String)}:
 *
 * <pre>{@code
 * FhirReader reader = FhirReader.of(spark, definitions);
 * reader.json().read("Patient", "/data/Patient.ndjson");
 * reader.xml().read("Patient", documents);
 * reader.json().readBundles("Observation", bundles);
 * }</pre>
 *
 * <p>How each format is parsed is not part of this API, and is free to change.
 */
public final class FhirReader {

  @Nonnull private final FhirJsonReader json;

  @Nonnull private final FhirXmlReader xml;

  private FhirReader(@Nonnull final FhirJsonReader json) {
    this.json = json;
    this.xml = new FhirXmlReader(json);
  }

  /**
   * Returns a reader that stores what it reads as the definitions describe.
   *
   * @param spark the Spark session to read files with
   * @param definitions the definitions that types, cardinality and field order are taken from
   * @return the reader
   */
  @Nonnull
  public static FhirReader of(
      @Nonnull final SparkSession spark, @Nonnull final DefinitionContext definitions) {
    return new FhirReader(new FhirJsonReader(spark, ResourceTransformer.of(definitions)));
  }

  /**
   * Returns the reader of FHIR JSON.
   *
   * @return the reader
   */
  @Nonnull
  public FhirJsonReader json() {
    return json;
  }

  /**
   * Returns the reader of FHIR XML.
   *
   * @return the reader
   */
  @Nonnull
  public FhirXmlReader xml() {
    return xml;
  }

  /**
   * Returns the reader of the format a media type names, which is {@code application/fhir+json} or
   * {@code application/fhir+xml}.
   *
   * @param mediaType the media type
   * @return the reader
   * @throws IllegalArgumentException if the media type names neither format
   */
  @Nonnull
  public FhirFormatReader format(@Nonnull final String mediaType) {
    return FhirMediaType.select(mediaType, json, xml);
  }
}
