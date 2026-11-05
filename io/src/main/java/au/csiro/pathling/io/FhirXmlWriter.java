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

import static au.csiro.pathling.utilities.SparkTypes.fieldOf;

import au.csiro.pathling.definition.DefinitionContext;
import au.csiro.pathling.schema.DefinitionCanonicalStructure;
import jakarta.annotation.Nonnull;
import java.util.Map;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * Writes resources stored in this layout as FHIR XML, to a dataset of documents or to files of
 * line-delimited XML (FR-045, decision 84). It is obtained from {@link FhirWriter#xml()}.
 *
 * <p>Each resource is shaped into the structure FHIR XML has and written with Spark's {@code
 * to_xml}. What is written is what the previous writer wrote, which converted the JSON writer's
 * output with the FHIR parser: what XML cannot carry of what the JSON writer writes is left out,
 * namely an element that is an empty structure and the null that keeps a repeating primitive's
 * position (decision 84). A decimal is written as the text it is stored as, which for one read from
 * XML is the text it was read as (owner, 2026-10-22). A narrative is written as the XHTML it holds,
 * which a stored narrative must be: one that is not well-formed fails the write, naming the
 * resource.
 */
public final class FhirXmlWriter implements FhirFormatWriter {

  /** An element written with no attributes and no content, which FHIR does not allow. */
  @Nonnull private static final String EMPTY_ELEMENT = XmlDocuments.EMPTY_ELEMENT.pattern();

  @Nonnull private static final String DOCUMENT = "document";

  @Nonnull private static final String NAME = "name";

  /** Removes the elements the XML writer wrote empty, and writes each narrative as XHTML. */
  @Nonnull
  private static final UserDefinedFunction FINISH =
      functions.udf(
          (UDF3<String, String, Boolean, String>) XmlDocuments::finish, DataTypes.StringType);

  @Nonnull private final DefinitionContext definitions;

  FhirXmlWriter(@Nonnull final DefinitionContext definitions) {
    this.definitions = definitions;
  }

  /**
   * {@inheritDoc}
   *
   * <p>A line feed in a narrative is written as it is, which XML allows, so a document may span
   * more than one line.
   */
  @Override
  @Nonnull
  public Dataset<String> write(
      @Nonnull final String resourceType, @Nonnull final Dataset<Row> stored) {
    return documents(resourceType, stored, false);
  }

  /**
   * Writes one FHIR XML document per stored resource to files of line-delimited XML, one document
   * per line, which {@link FhirXmlReader#read(String, String)} reads back.
   *
   * <p>Spark's XML writer writes a line feed in an attribute as a character reference, and a line
   * feed in a narrative's text is written as one too, {@code &#xa;}, which parses back to the same
   * character. A line break inside a narrative's comment, CDATA section or processing instruction
   * cannot be written that way, and fails the write, naming the resource.
   *
   * @param resourceType the type of the resources the dataset carries
   * @param stored the resources, in this layout
   * @param path the directory to write to
   * @param mode what to do where the directory already exists
   */
  public void write(
      @Nonnull final String resourceType,
      @Nonnull final Dataset<Row> stored,
      @Nonnull final String path,
      @Nonnull final SaveMode mode) {
    documents(resourceType, stored, true).write().mode(mode).text(path);
  }

  @Nonnull
  private Dataset<String> documents(
      @Nonnull final String resourceType,
      @Nonnull final Dataset<Row> stored,
      final boolean lineDelimited) {
    final StructType schema = stored.schema();
    final DefinitionCanonicalStructure canonical =
        DefinitionCanonicalStructure.forResource(definitions, resourceType);
    final Column xml =
        functions.rtrim(
            functions.to_xml(
                XmlEgressShape.shape(canonical, schema),
                Map.of("rowTag", resourceType, "indent", "")),
            "\n");
    final Column name =
        fieldOf(schema, "id").isPresent()
            ? functions.concat(
                functions.lit(resourceType + " with id \""),
                functions.col("id"),
                functions.lit("\""))
            : functions.lit(resourceType);
    // The document is written once, in a projection of its own, and finished only where it holds
    // an empty element or a narrative, which few documents do.
    final Dataset<Row> written = stored.select(xml.alias(DOCUMENT), name.alias(NAME));
    final Column document = functions.col(DOCUMENT);
    final Column result =
        functions
            .when(
                document.rlike(EMPTY_ELEMENT).or(document.contains(XmlDocuments.NARRATIVE_START)),
                FINISH.apply(document, functions.col(NAME), functions.lit(lineDelimited)))
            .otherwise(document);
    return written.select(result.alias(DOCUMENT)).as(Encoders.STRING());
  }
}
