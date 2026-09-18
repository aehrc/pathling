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

package au.csiro.pathling.io.egress;

import au.csiro.pathling.definition.DefinitionContext;
import au.csiro.pathling.definition.ElementDefinition;
import au.csiro.pathling.io.transform.DecimalTransform;
import au.csiro.pathling.schema.DefinitionCanonicalStructure;
import au.csiro.pathling.schema.LayoutEntry;
import au.csiro.pathling.schema.LayoutFields;
import au.csiro.pathling.schema.PrimitiveTypes;
import jakarta.annotation.Nonnull;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Writes resources stored in this layout back out as FHIR JSON (FR-016, FR-019).
 *
 * <p>Like the ingest, this is dataset operations over public Spark API and nothing else: the
 * document is built as a column expression and written by the JSON writer, rather than by handing a
 * row to a FHIR parser in a per-row plan (FR-050, FR-051).
 *
 * <p>The definitions are consulted rather than the schema alone, because the schema cannot say what
 * a column means. A decimal and a code are both text in the layout, and only one of them is a
 * number in the document.
 *
 * <p>Two kinds of field are left out of the document. Annotations, which are this layout's own
 * material and no part of FHIR. And the metadata groups carrying the ids and extensions of
 * primitives, which nothing populates before M5 — they are null in every row until then, so leaving
 * them out changes nothing that is written, and T078c is where they start being written and this
 * exclusion comes out.
 */
public final class ResourceSerialiser {

  /** Asks the JSON writer to leave out a field that is null, which is most of FR-019. */
  @Nonnull
  private static final Map<String, String> WRITE_OPTIONS = Map.of("ignoreNullFields", "true");

  @Nonnull private static final String DOCUMENT = "document";

  @Nonnull private final DefinitionContext definitions;

  private ResourceSerialiser(@Nonnull final DefinitionContext definitions) {
    this.definitions = definitions;
  }

  /**
   * Returns a serialiser over a set of definitions.
   *
   * @param definitions the definitions the layout was derived from
   * @return the serialiser
   */
  @Nonnull
  public static ResourceSerialiser of(@Nonnull final DefinitionContext definitions) {
    return new ResourceSerialiser(definitions);
  }

  /**
   * Returns one FHIR JSON document per stored resource.
   *
   * @param resourceType the type of the resources the dataset carries
   * @param stored the resources, in this layout
   * @return the documents
   */
  @Nonnull
  public Dataset<String> serialise(
      @Nonnull final String resourceType, @Nonnull final Dataset<Row> stored) {
    final DefinitionCanonicalStructure canonical =
        DefinitionCanonicalStructure.forResource(definitions, resourceType);
    final Column[] fields = fields(canonical, stored.schema(), functions::col);
    // The resource itself is never pruned away: it carries its type, so it is never empty.
    final Column document = functions.to_json(functions.struct(fields), WRITE_OPTIONS);
    return stored
        .select(DecimalTransform.unmarkedDocument(document).alias(DOCUMENT))
        .as(Encoders.STRING());
  }

  /** Returns the fields of one structure of the document, in the order the layout stores them. */
  @Nonnull
  private Column[] fields(
      @Nonnull final DefinitionCanonicalStructure node,
      @Nonnull final StructType schema,
      @Nonnull final Function<String, Column> source) {
    return Stream.of(schema.fields())
        .filter(field -> written(node, field))
        .map(field -> field(node, field, source.apply(field.name())).alias(field.name()))
        .toArray(Column[]::new);
  }

  /** Answers whether a stored field belongs in the document at all. */
  private static boolean written(
      @Nonnull final DefinitionCanonicalStructure node, @Nonnull final StructField field) {
    final LayoutEntry entry = entry(node, field);
    return entry.isResourceType() || entry.isElement();
  }

  /** Returns the value of one field of the document. */
  @Nonnull
  private Column field(
      @Nonnull final DefinitionCanonicalStructure node,
      @Nonnull final StructField field,
      @Nonnull final Column stored) {
    if (LayoutFields.RESOURCE_TYPE.equals(field.name())) {
      return stored;
    }
    return value(node, entry(node, field), stored, field.dataType());
  }

  /**
   * Returns the value of one element of the document, descending through an array where the element
   * repeats.
   */
  @Nonnull
  private Column value(
      @Nonnull final DefinitionCanonicalStructure node,
      @Nonnull final LayoutEntry entry,
      @Nonnull final Column stored,
      @Nonnull final DataType type) {
    if (type instanceof final ArrayType array) {
      return EmptyPruning.array(
          functions.transform(stored, element -> value(node, entry, element, array.elementType())));
    }
    final FHIRDefinedType fhirType =
        entry.getElement().flatMap(ElementDefinition::getFhirType).orElseThrow();
    if (FHIRDefinedType.DECIMAL.equals(fhirType)) {
      return DecimalTransform.markedValue(stored);
    }
    if (PrimitiveTypes.isPrimitive(fhirType)) {
      // The JSON writer renders a boolean and an integer as themselves, because the definitions
      // made them a boolean and an integer column on the way in.
      return stored;
    }
    final DefinitionCanonicalStructure child = node.elementStructure(entry).orElseThrow();
    return EmptyPruning.structure(fields(child, (StructType) type, stored::getField));
  }

  @Nonnull
  private static LayoutEntry entry(
      @Nonnull final DefinitionCanonicalStructure node, @Nonnull final StructField field) {
    return node.entry(field.name())
        .orElseThrow(
            () ->
                new IllegalStateException(
                    "Stored field is not in the canonical structure: " + field.name()));
  }
}
