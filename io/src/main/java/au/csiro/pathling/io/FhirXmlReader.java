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
 * Reads FHIR XML into this layout, from a dataset of documents or a dataset of bundles (FR-045,
 * decision 84). It is obtained from {@link FhirReader#xml()}.
 *
 * <p>Cardinality comes from the definitions, as it does for JSON: a repeating element that occurs
 * once is stored as an array. A narrative is stored as the XML wrote it.
 *
 * <p>Unlike JSON, XML is parsed with the FHIR parser rather than read by Spark, as it was by the
 * previous encoder, and that shows in three ways (decision 83). Content the definitions do not
 * describe is dropped without being reported. Content that is not conformant but can be read, such
 * as two values for one choice element, is coerced without being reported. And content the parser
 * cannot read, such as an invalid primitive value or an unknown resource type, fails the job that
 * evaluates the result, where JSON reports it and continues.
 */
public final class FhirXmlReader implements FhirFormatReader {

  @Nonnull private final FhirJsonReader json;

  @Nonnull private final FhirParsers parsers;

  FhirXmlReader(@Nonnull final FhirJsonReader json, @Nonnull final FhirParsers parsers) {
    this.json = json;
    this.parsers = parsers;
  }

  /**
   * {@inheritDoc}
   *
   * <p>A document whose resource is of another type is left out, as the previous encoder left it
   * out, and so is a null document. A bundle is a document of another type, and so is left out
   * rather than exploded; {@link #readBundles} explodes it.
   */
  @Override
  @Nonnull
  public Dataset<Row> read(
      @Nonnull final String resourceType, @Nonnull final Dataset<String> documents) {
    return json.read(resourceType, XmlConversion.toJson(resourceType, documents, parsers));
  }

  @Override
  @Nonnull
  public Dataset<Row> readBundles(
      @Nonnull final String resourceType, @Nonnull final Dataset<String> bundles) {
    return json.read(resourceType, BundleTransformer.xml(parsers).resources(resourceType, bundles));
  }
}
