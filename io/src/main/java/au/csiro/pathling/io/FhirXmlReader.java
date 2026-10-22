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
import au.csiro.pathling.schema.DefinitionCanonicalStructure;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * Reads FHIR XML into this layout, from files of line-delimited XML, from a dataset of documents,
 * or from a dataset of bundles (FR-045, decision 84). It is obtained from {@link FhirReader#xml()}.
 *
 * <p>What is stored is what JSON ingest stores for the same resources (FR-060, decision 134), but
 * for a decimal, which is stored as the text the XML carries, where JSON passes it through a double
 * (owner, 2026-10-22). Each document is parsed with Spark's {@code from_xml}, and shaped into the
 * JSON data model by a plan generated from the definitions, so it meets the same transform as JSON.
 * The elements present are learnt by one scan with Spark's XML inference, every value read as text,
 * as JSON's are with Spark's JSON inference. Cardinality and types come from the definitions, so a
 * repeating element that occurs once is stored as an array.
 *
 * <p>Ingest is in {@code trust} (owner, 2026-10-22): what does not conform is not checked, and what
 * is not stored is left out without a finding. A narrative is held to L2: written back out, it
 * parses to the same XHTML as the source. It is taken from the document by a streaming parse rather
 * than by Spark, which cannot tell markup from escaped text. The read fails, naming the document,
 * only where carrying on would be unsafe or would leave what the data is ambiguous: text after the
 * root or a second root, a document type declaration, and a root outside the FHIR namespace. A
 * document in the FHIR namespace under a prefix is read, and an element in another namespace,
 * outside a narrative, is left out rather than read as FHIR.
 */
public final class FhirXmlReader implements FhirFormatReader {

  /**
   * The options of the scan that learns which elements are present. Every value is read as text,
   * since its type comes from the definitions. Surrounding whitespace in a value is kept, as JSON
   * keeps it.
   */
  @Nonnull
  private static final Map<String, String> INFER =
      Map.of("inferSchema", "false", "ignoreSurroundingSpaces", "false", "mode", "FAILFAST");

  /** The options of the parse of each document. */
  @Nonnull
  private static final Map<String, String> PARSE =
      Map.of("ignoreSurroundingSpaces", "false", "mode", "FAILFAST");

  /** The option naming the root element of each document. */
  @Nonnull private static final String ROW_TAG = "rowTag";

  @Nonnull private static final String SHAPED = "shaped";

  /**
   * Checks a document and prepares it for Spark, as an array of one, or of none to leave it out.
   */
  @Nonnull
  private static final UserDefinedFunction PREPARE =
      functions.udf(
          (UDF2<String, String, List<String>>)
              (document, resourceType) -> {
                final String prepared = XmlDocuments.prepare(document, resourceType);
                return prepared == null ? List.of() : List.of(prepared);
              },
          DataTypes.createArrayType(DataTypes.StringType, false));

  /** Splits a bundle into the resources of one type it carries. */
  @Nonnull
  private static final UserDefinedFunction SPLIT =
      functions.udf(
          (UDF2<String, String, List<Row>>)
              (bundle, resourceType) ->
                  XmlDocuments.split(bundle, resourceType).stream()
                      .map(BundleResource::toRow)
                      .toList(),
          DataTypes.createArrayType(BundleResource.TYPE, false));

  @Nonnull private final SparkSession spark;

  @Nonnull private final ResourceTransformer transformer;

  @Nonnull private final DefinitionContext definitions;

  FhirXmlReader(
      @Nonnull final SparkSession spark,
      @Nonnull final ResourceTransformer transformer,
      @Nonnull final DefinitionContext definitions) {
    this.spark = spark;
    this.transformer = transformer;
    this.definitions = definitions;
  }

  /**
   * Reads files of line-delimited FHIR XML, one document per line, into this layout. A line feed
   * within a document is written as a character reference in such a file, which is how {@link
   * FhirXmlWriter#write(String, Dataset, String, org.apache.spark.sql.SaveMode)} writes one.
   *
   * @param resourceType the type of the resources to read
   * @param path the path to read from, which may name a file or a directory
   * @return the resources of that type, in this layout
   * @throws IllegalArgumentException if the type is {@code Bundle} or is not a resource type
   */
  @Nonnull
  public Dataset<Row> read(@Nonnull final String resourceType, @Nonnull final String path) {
    transformer.requireStorable(resourceType);
    return read(resourceType, spark.read().textFile(path));
  }

  /**
   * {@inheritDoc}
   *
   * <p>A document whose resource is of another type is left out, as the previous encoder left it
   * out, and so is a null or blank document, such as the empty line between two documents of a
   * line-delimited file. A bundle is a document of another type, and so is left out rather than
   * exploded; {@link #readBundles} explodes it. A document of another type is checked all the same,
   * and fails the read where it is not well-formed FHIR XML.
   */
  @Override
  @Nonnull
  public Dataset<Row> read(
      @Nonnull final String resourceType, @Nonnull final Dataset<String> documents) {
    transformer.requireStorable(resourceType);
    final Dataset<Row> prepared =
        documents.select(
            functions
                .explode(
                    PREPARE.apply(
                        functions.col(documents.columns()[0]), functions.lit(resourceType)))
                .alias(BundleResource.DOCUMENT));
    return toLayout(resourceType, prepared, null);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Each bundle is read with a streaming parse, which takes out the resources of the type asked
   * for, each then read as a document of its own. A document that is not a bundle fails the read,
   * as does one that is not well-formed FHIR XML.
   */
  @Override
  @Nonnull
  public Dataset<Row> readBundles(
      @Nonnull final String resourceType, @Nonnull final Dataset<String> bundles) {
    transformer.requireStorable(resourceType);
    return toLayout(
        resourceType,
        BundleResource.resources(bundles, SPLIT, resourceType),
        functions.col(BundleResource.REFERENCES));
  }

  /**
   * Stores documents of one type, prepared for Spark, resolving their references through the
   * resolutions beside them where they came from a bundle.
   */
  @Nonnull
  private Dataset<Row> toLayout(
      @Nonnull final String resourceType,
      @Nonnull final Dataset<Row> documents,
      @Nullable final Column references) {
    final StructType inferred =
        documents
            .sparkSession()
            .read()
            .options(INFER)
            .option(ROW_TAG, resourceType)
            .xml(documents.select(BundleResource.DOCUMENT).as(Encoders.STRING()))
            .schema();
    final XmlIngestShape shape =
        XmlIngestShape.of(
            DefinitionCanonicalStructure.forResource(definitions, resourceType),
            resourceType,
            inferred);
    final Column document = functions.col(BundleResource.DOCUMENT);
    final boolean parsing = shape.parseSchema().fields().length > 0;
    // Each document is parsed once, in a projection of its own, and shaped from what it holds.
    final Dataset<Row> parsed =
        parsing
            ? documents.withColumn(
                BundleResource.PARSED, functions.from_xml(document, shape.parseSchema(), PARSE))
            : documents;
    final Column shaped =
        shape.shape(parsing ? functions.col(BundleResource.PARSED) : null, document);
    // The shaped resource is a column of its own, so that it is planned once.
    final Dataset<Row> shapedRows =
        references == null
            ? parsed.select(shaped.alias(SHAPED))
            : parsed.select(shaped.alias(SHAPED), references);
    final StructType observed = (StructType) shapedRows.schema().apply(SHAPED).dataType();
    final UnaryOperator<Column> resolution =
        references == null
            ? UnaryOperator.identity()
            : reference -> BundleResource.resolve(reference, references);
    return shapedRows.select(
        transformer.toLayout(resourceType, observed, functions.col(SHAPED), resolution));
  }
}
