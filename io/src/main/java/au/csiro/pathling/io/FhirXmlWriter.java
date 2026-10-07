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

import jakarta.annotation.Nonnull;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Writes resources stored in this layout as FHIR XML, one document per resource (FR-045, decision
 * 84). It is obtained from {@link FhirWriter#xml()}.
 *
 * <p>Each resource is written as FHIR JSON first and then converted with the FHIR parser, as the
 * previous encoder's decoding wrote XML with it. The conversion drops what the JSON writes but XML
 * cannot carry: an element that is an empty structure, and the null that keeps a repeating
 * primitive's position. A narrative's whitespace beside a tag is collapsed to one space, and a
 * decimal is written from the double the JSON writer emits, so it may carry trailing zeros its
 * source did not (decisions 68 and 84).
 */
public final class FhirXmlWriter implements FhirFormatWriter {

  @Nonnull private final FhirJsonWriter json;

  FhirXmlWriter(@Nonnull final FhirJsonWriter json) {
    this.json = json;
  }

  @Override
  @Nonnull
  public Dataset<String> write(
      @Nonnull final String resourceType, @Nonnull final Dataset<Row> stored) {
    return XmlConversion.toXml(json.write(resourceType, stored));
  }
}
