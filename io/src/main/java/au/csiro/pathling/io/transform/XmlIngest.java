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

package au.csiro.pathling.io.transform;

import static org.apache.spark.sql.functions.col;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

/**
 * Converts FHIR XML to FHIR JSON, so that XML reaches the layout through the same JSON read and
 * transform as JSON does (FR-045).
 *
 * <p>Each document is parsed with HAPI and written out as JSON, inside a user-defined function. The
 * conversion is the FHIR parser's rather than Spark's: Spark's own XML reader infers the shape of
 * each element from the text, and a repeating element that occurs once looks in XML exactly like
 * one that cannot repeat, so it would be inferred as a single value. Here a primitive's extension,
 * which XML carries as a child element, becomes the underscore-prefixed sibling JSON uses, and
 * cardinality comes from the definitions. The output is then read like any other FHIR JSON, by
 * {@code FhirJsonReader}, and is subject to the same one-type-per-input contract (decision 70).
 *
 * <p>A FHIR object in the per-row plan is a deliberate exception to FR-050 that plan.md records,
 * because no Spark-native path preserves FHIR semantics for this format. HAPI's parser ignores
 * content the definitions do not describe, so such content in XML is not reported as a finding the
 * way it is for JSON (decision 83).
 *
 * <p>A bundle converts like any other resource, to a bundle written as JSON. Its entries are not
 * exploded or resolved here; {@link BundleTransformer#xml()} does both for a bundle written as XML.
 */
public final class XmlIngest {

  /** The name given to the one column of a dataset of documents, whatever it was called. */
  @Nonnull private static final String VALUE = "value";

  private XmlIngest() {}

  /**
   * Returns the conversion.
   *
   * @return the conversion
   */
  @Nonnull
  public static XmlIngest of() {
    return new XmlIngest();
  }

  /**
   * Converts a column of FHIR XML documents to FHIR JSON. A null document converts to null, and a
   * document that is not FHIR XML fails the job that evaluates the result.
   *
   * @param documents the column of XML documents
   * @return the column of JSON documents
   */
  @Nonnull
  public Column toJson(@Nonnull final Column documents) {
    return conversion().apply(documents);
  }

  /**
   * Converts a dataset of FHIR XML documents to FHIR JSON, one document per row.
   *
   * @param documents the XML documents
   * @return the JSON documents, in the same order
   */
  @Nonnull
  public Dataset<String> toJson(@Nonnull final Dataset<String> documents) {
    return documents.toDF(VALUE).select(toJson(col(VALUE))).as(Encoders.STRING());
  }

  @Nonnull
  private static UserDefinedFunction conversion() {
    return functions.udf((UDF1<String, String>) XmlIngest::convert, DataTypes.StringType);
  }

  @Nullable
  private static String convert(@Nullable final String document) {
    return document == null
        ? null
        : FhirParsers.json().encodeResourceToString(FhirParsers.xml().parseResource(document));
  }
}
