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
import jakarta.annotation.Nullable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.hl7.fhir.instance.model.api.IBaseResource;

/**
 * Converts between FHIR XML and FHIR JSON, so that XML reaches and leaves the layout through the
 * same JSON read and write as JSON does (FR-045, decision 84).
 *
 * <p>Each document is parsed with HAPI and written out in the other format, inside the function
 * Spark runs. The conversion is the FHIR parser's rather than Spark's: Spark's own XML reader
 * infers the shape of each element from the text, and a repeating element that occurs once looks in
 * XML exactly like one that cannot repeat, so it would be inferred as a single value. Here a
 * primitive's extension, which XML carries as a child element, becomes the underscore-prefixed
 * sibling JSON uses, and cardinality comes from the definitions.
 *
 * <p>A FHIR object in the per-row plan is a deliberate exception to FR-050 that plan.md records,
 * because no Spark-native path preserves FHIR semantics for this format. HAPI's parser ignores
 * content the definitions do not describe, so such content in XML is not reported as a finding the
 * way it is for JSON (decision 83). None of this is public: {@link FhirXmlReader} and {@link
 * FhirXmlWriter} are, and the mechanism behind them may change.
 */
final class XmlConversion {

  private XmlConversion() {}

  /**
   * Converts FHIR XML documents to FHIR JSON, keeping only the resources of one type. A null
   * document converts to nothing, and a document that is not FHIR XML fails the job that evaluates
   * the result.
   *
   * <p>The selection is made here because the type of an XML document is known only once it is
   * parsed, and the JSON reader's input carries resources of one type (decision 70). It keeps what
   * the previous encoder kept, which also discarded documents of other types.
   *
   * @param resourceType the type of the resources to keep
   * @param documents the XML documents, one per row
   * @return the JSON documents of that type, in the same order
   */
  @Nonnull
  static Dataset<String> toJson(
      @Nonnull final String resourceType, @Nonnull final Dataset<String> documents) {
    return toJson(resourceType, documents, FhirParsers.standard());
  }

  /**
   * Converts FHIR XML documents to FHIR JSON as {@link #toJson(String, Dataset)} does, reading the
   * resource types the parsers read.
   *
   * @param resourceType the type of the resources to keep
   * @param documents the XML documents, one per row
   * @param parsers the parsers to convert the documents with
   * @return the JSON documents of that type, in the same order
   */
  @Nonnull
  static Dataset<String> toJson(
      @Nonnull final String resourceType,
      @Nonnull final Dataset<String> documents,
      @Nonnull final FhirParsers parsers) {
    return documents.flatMap(
        (FlatMapFunction<String, String>)
            document -> convertToJson(document, resourceType, parsers),
        Encoders.STRING());
  }

  /**
   * Converts FHIR JSON documents to FHIR XML, one document per row.
   *
   * @param documents the JSON documents
   * @param parsers the parsers to convert the documents with
   * @return the XML documents, in the same order
   */
  @Nonnull
  static Dataset<String> toXml(
      @Nonnull final Dataset<String> documents, @Nonnull final FhirParsers parsers) {
    return documents.map(
        (MapFunction<String, String>)
            document ->
                parsers.xml().encodeResourceToString(parsers.json().parseResource(document)),
        Encoders.STRING());
  }

  @Nonnull
  private static Iterator<String> convertToJson(
      @Nullable final String document,
      @Nonnull final String resourceType,
      @Nonnull final FhirParsers parsers) {
    if (document == null) {
      return Collections.emptyIterator();
    }
    final IBaseResource resource = parsers.xml().parseResource(document);
    return resourceType.equals(resource.fhirType())
        ? List.of(parsers.json().encodeResourceToString(resource)).iterator()
        : Collections.emptyIterator();
  }
}
