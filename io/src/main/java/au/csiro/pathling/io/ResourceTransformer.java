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

import static au.csiro.pathling.utilities.SparkTypes.elementTypeOf;
import static au.csiro.pathling.utilities.SparkTypes.fieldOf;

import au.csiro.pathling.definition.DefinitionContext;
import au.csiro.pathling.definition.ElementDefinition;
import au.csiro.pathling.definition.NodeDefinition;
import au.csiro.pathling.schema.DefinitionCanonicalStructure;
import au.csiro.pathling.schema.LayoutEntry;
import au.csiro.pathling.schema.LayoutFields;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Transforms FHIR JSON that has already been parsed into a dataset into this layout, and this
 * layout back into a dataset that serialises as FHIR JSON (R-008, decision 70).
 *
 * <p>Both directions are structured in and structured out: no text is read or written here, which
 * is what {@link FhirJsonReader} and {@link FhirJsonWriter} add. This class is not public, because
 * its input is the intermediate a JSON read infers, which a later reader may not produce (decision
 * 84). Types, cardinality, conventions and field order all come from the definitions, and the
 * schema of the input decides nothing but which elements the data populates (FR-008, FR-012). The
 * schema is fitted to the data because presence can only be known after seeing it.
 *
 * <p>The input to {@link #toLayout} is in the JSON data model: every element under its FHIR JSON
 * name, a structure as a structure, a repeating element as an array, and each primitive in the type
 * Spark's JSON reader infers for it or a lossless widening of that type (see {@link
 * PrimitiveConverters}). The input is assumed to conform to the definitions, and nothing here
 * checks that it does, which is ingest in {@code trust} (decision 133's addendum of 2026-10-22).
 * Each declared element is cast or extracted from what the source carries as though it conformed.
 * Where it does not, the outcome is undefined (FR-063): the read may fail, with Spark's error or
 * with an exception while it is planned, a value may become null or be stored wrong, and an element
 * may be stored in a shape other than the one the definitions declare. Nothing is added to handle
 * any of these. Content the definitions do not describe has no column in the layout, and is left
 * out.
 *
 * <p>Neither direction removes what is empty, and neither checks for it (decision 71). Conformant
 * input still leaves two kinds of emptiness, both kept on purpose until M5 stores primitive
 * metadata: a structure whose only content was a primitive's id and extensions, which is an element
 * that exists and is written as an empty object whatever other conformant documents the input holds
 * (decision 72), and a repeating primitive's positional null, which is written without the metadata
 * it aligns with (decision 71's addendum). Anything else that holds nothing is assumed absent, such
 * as a structure whose every field is null in layout data from another producer. Where that is
 * broken the document written from the result is not conformant FHIR, because such an element is
 * written as an empty object rather than left out.
 *
 * <p>The whole transform is dataset operations over public Spark API. No FHIR object appears in a
 * per-row plan and no expression tree is authored by hand (FR-050, FR-051).
 */
final class ResourceTransformer {

  /** The name of the field a decimal of an element is marked with, which no FHIR name can be. */
  @Nonnull private static final String DECIMAL_MARKER = "$decimal$";

  /** A marked decimal as the JSON writer writes it, capturing the decimal's text. */
  @Nonnull
  private static final String MARKED_DECIMAL = "\\{\"\\$decimal\\$\":\"(-?[0-9][0-9.eE+-]*)\"\\}";

  /** The element of a Reference that holds the reference itself. */
  @Nonnull private static final String REFERENCE = "reference";

  @Nonnull private final DefinitionContext definitions;

  private ResourceTransformer(@Nonnull final DefinitionContext definitions) {
    this.definitions = definitions;
  }

  /**
   * Returns a transformer over a set of definitions.
   *
   * @param definitions the definitions that types, cardinality and field order are taken from
   * @return the transformer
   */
  @Nonnull
  public static ResourceTransformer of(@Nonnull final DefinitionContext definitions) {
    return new ResourceTransformer(definitions);
  }

  /**
   * Transforms FHIR JSON parsed into a dataset into this layout.
   *
   * <p>One walk over the definitions builds every column, and the stored schema is whatever those
   * columns produce: it is never constructed, so it cannot disagree with what is written (decision
   * 68). The schema of the source is threaded through the walk and decides presence alone. An
   * element appears where the source carried it, and a structure appears only where some element
   * beneath it does (FR-011). What the source carries and this layout does not store is left out.
   *
   * @param resourceType the type of the resources the source carries
   * @param source the resources, in the JSON data model
   * @return the resources, in this layout
   * @throws IllegalArgumentException if the type is {@code Bundle} or is not a resource type
   */
  @Nonnull
  public Dataset<Row> toLayout(
      @Nonnull final String resourceType, @Nonnull final Dataset<Row> source) {
    return source.select(
        layoutFields(resourceType, source.schema(), functions::col, UnaryOperator.identity()));
  }

  /**
   * Transforms resources held as a structure in the JSON data model into this layout, as {@link
   * #toLayout(String, Dataset)} does for a dataset of them, returning the columns that method would
   * select. It serves the routes that build the JSON data model themselves, from another format or
   * from the resources of a bundle (ticket 32). The structure is best a column of its own, computed
   * in a projection before these columns are selected, so that it is planned once rather than
   * within every column.
   *
   * <p>Every stored {@code Reference.reference} is passed through a function, which is how a bundle
   * route resolves the references between the bundle's entries on the columns the definitions type
   * as references, and nowhere else (decision 83, FR-062). An element of another type that happens
   * to be named {@code reference} is not passed through it.
   *
   * @param resourceType the type of the resources
   * @param observed the type of the structure, which decides presence alone
   * @param source the structure
   * @param references the function every stored reference is passed through
   * @return the columns of the resources, in this layout
   * @throws IllegalArgumentException if the type is {@code Bundle} or is not a resource type
   */
  @Nonnull
  Column[] toLayout(
      @Nonnull final String resourceType,
      @Nonnull final StructType observed,
      @Nonnull final Column source,
      @Nonnull final UnaryOperator<Column> references) {
    return layoutFields(resourceType, observed, source::getField, references);
  }

  /** Returns the stored columns of a resource whose fields are reached through a function. */
  @Nonnull
  private Column[] layoutFields(
      @Nonnull final String resourceType,
      @Nonnull final StructType observed,
      @Nonnull final Function<String, Column> fields,
      @Nonnull final UnaryOperator<Column> references) {
    requireStorable(resourceType);
    final DefinitionCanonicalStructure canonical =
        DefinitionCanonicalStructure.forResource(definitions, resourceType);
    return canonical.entries().stream()
        .map(entry -> resourceField(canonical, entry, observed, resourceType, fields, references))
        .flatMap(Optional::stream)
        .toArray(Column[]::new);
  }

  /**
   * Refuses a resource type that may not be stored, which every read asks before it infers a schema
   * and {@link #toLayout} asks again (FR-007).
   *
   * @param resourceType the resource type to check
   * @throws IllegalArgumentException if the type is {@code Bundle} or is not a resource type
   */
  void requireStorable(@Nonnull final String resourceType) {
    StorableResourceType.require(definitions, resourceType);
  }

  /**
   * Transforms resources in this layout into the dataset the JSON writer serialises as FHIR JSON,
   * one top-level column per field of the document (FR-016, FR-019).
   *
   * <p>The definitions are consulted rather than the schema alone, because the schema cannot say
   * what a column means: a decimal and a code are both text in the layout, and only one of them is
   * a number in the document. A structure that is null in the layout is returned as a null, so that
   * an absent element is not rebuilt as a present one; nothing else is examined, and what is empty
   * is written as it stands (decision 71). Leaving out the fields that are null is the writer's
   * part.
   *
   * <p>The result is for serialising and not for storing. A decimal is returned as a double, which
   * is what the JSON writer emits as a bare number (decision 68), so a copy written to Parquet
   * would hold it at a double's precision.
   *
   * <p>Two kinds of field are left out. Annotations, which are this layout's own material and no
   * part of FHIR. And the metadata groups carrying the ids and extensions of primitives, which
   * nothing populates before M5 — they are null in every row until then, so leaving them out
   * changes nothing that is written, and T078c is where they start being written and this exclusion
   * comes out.
   *
   * @param resourceType the type of the resources the dataset carries
   * @param stored the resources, in this layout
   * @return the resources, in the JSON data model
   */
  @Nonnull
  public Dataset<Row> toJsonShape(
      @Nonnull final String resourceType, @Nonnull final Dataset<Row> stored) {
    final DefinitionCanonicalStructure canonical =
        DefinitionCanonicalStructure.forResource(definitions, resourceType);
    return stored.select(egressFields(canonical, stored.schema(), functions::col, false));
  }

  /**
   * Transforms the values of one element, in this layout, into what the JSON writer serialises as
   * FHIR JSON, as {@link #toJsonShape} does for a resource (T100j).
   *
   * <p>The values are a structure, or an array of them, of the type the definition describes. A
   * field the definition does not describe is left out rather than refused, because a value
   * computed by a query can carry fields of its own beside the elements: the scale and canonical
   * form of a quantity the engine builds, or a field identifier of the previous layout. Which
   * fields are written is decided by the definition, never by a field's name. A decimal is marked
   * rather than given as a double, for {@link #unmarkDecimals} to write it with its text.
   *
   * @param element the definition of the element
   * @param stored the values
   * @param type the type of the values, a structure or an array of them
   * @return the values, in the JSON data model
   * @throws IllegalArgumentException if the type is neither a structure nor an array of them
   */
  @Nonnull
  static Column elementToJsonShape(
      @Nonnull final NodeDefinition element,
      @Nonnull final Column stored,
      @Nonnull final DataType type) {
    final DefinitionCanonicalStructure node = DefinitionCanonicalStructure.of(element, false);
    return elementValue(node, stored, type);
  }

  @Nonnull
  private static Column elementValue(
      @Nonnull final DefinitionCanonicalStructure node,
      @Nonnull final Column stored,
      @Nonnull final DataType type) {
    if (type instanceof final ArrayType array) {
      return functions.transform(stored, value -> elementValue(node, value, array.elementType()));
    }
    if (type instanceof final StructType structure) {
      return functions.when(
          stored.isNotNull(),
          functions.struct(egressFields(node, structure, stored::getField, true)));
    }
    throw new IllegalArgumentException(
        "Expected a structure, or an array of structures, but the values are: "
            + type.simpleString());
  }

  /**
   * Returns the column for one field of a resource, or empty where nothing is stored for it. The
   * field naming the resource type has no element behind it, so it is the one field taken from the
   * source without consulting a definition, and it falls back to the type the caller named where a
   * source omits it.
   */
  @Nonnull
  private Optional<Column> resourceField(
      @Nonnull final DefinitionCanonicalStructure canonical,
      @Nonnull final LayoutEntry entry,
      @Nonnull final StructType observed,
      @Nonnull final String resourceType,
      @Nonnull final Function<String, Column> fields,
      @Nonnull final UnaryOperator<Column> references) {
    if (entry.isResourceType()) {
      final Column column =
          fieldOf(observed, entry.getName())
              .map(unused -> fields.apply(entry.getName()).cast(DataTypes.StringType))
              .orElseGet(() -> functions.lit(resourceType));
      return Optional.of(column.alias(entry.getName()));
    }
    return field(canonical, entry, observed, references, false).map(stored -> stored.apply(fields));
  }

  /**
   * Plans the column for one field of a structure, or returns empty where nothing is stored for it.
   * What is planned is a function from the way a node's fields are reached in the source to the
   * column, so that the same plan serves a field of the resource, of a structure and of each value
   * of an array.
   *
   * <p>The plan depends on the schema alone, so it is made once for every row. Annotations and
   * metadata groups are not written before M5, so neither is ever stored here. An element the
   * source did not carry does not appear. A primitive the source carried only as its metadata group
   * is stored as a null of its declared type, so that the element holding it stays present.
   *
   * @param node the canonical structure of the node carrying the field
   * @param entry the field
   * @param observed the structure the source was read with at this node
   */
  @Nonnull
  private Optional<Function<Function<String, Column>, Column>> field(
      @Nonnull final DefinitionCanonicalStructure node,
      @Nonnull final LayoutEntry entry,
      @Nonnull final StructType observed,
      @Nonnull final UnaryOperator<Column> references,
      final boolean inReference) {
    if (!entry.isElement()) {
      return Optional.empty();
    }
    final String name = entry.getName();
    // The reference of a Reference is the one field a bundle's resolution applies to.
    final UnaryOperator<Column> resolution =
        inReference && REFERENCE.equals(name) ? references : UnaryOperator.identity();
    return fieldOf(observed, name)
        .flatMap(
            from ->
                value(node, entry, from.dataType(), references)
                    .<Function<Function<String, Column>, Column>>map(
                        stored -> fields -> resolution.apply(stored.apply(fields.apply(name)))))
        .or(() -> metadataOnly(entry, observed))
        .map(stored -> fields -> stored.apply(fields).alias(name));
  }

  /**
   * Plans a null of the type the definitions give a primitive the source carried only as its id and
   * extensions, or returns empty where the source carried no metadata group for it.
   *
   * <p>The metadata group is not stored before M5, but the element it belongs to exists, and a
   * structure holding nothing else is an element that exists too. Storing the primitive as a null
   * keeps that structure present whatever other conformant documents the input holds (decision 72).
   * For a singular primitive this is what the released encoder stores. For a repeating one it is
   * not, because the parser the released encoder reads through discards an {@code _x} array with no
   * {@code x} array beside it; the layout keeps the element, as FHIR does. The metadata itself is
   * not stored.
   */
  @Nonnull
  private static Optional<Function<Function<String, Column>, Column>> metadataOnly(
      @Nonnull final LayoutEntry entry, @Nonnull final StructType observed) {
    return entry
        .primitiveType()
        .filter(
            type -> fieldOf(observed, LayoutFields.metadataGroupName(entry.getName())).isPresent())
        .map(type -> absent(entry, type))
        .map(column -> fields -> column);
  }

  /**
   * Plans the stored value of an element the source carried in the shape the definitions declare,
   * descending through an array where the element repeats.
   */
  @Nonnull
  private Optional<Function<Column, Column>> value(
      @Nonnull final DefinitionCanonicalStructure node,
      @Nonnull final LayoutEntry entry,
      @Nonnull final DataType observed,
      @Nonnull final UnaryOperator<Column> references) {
    final DataType observedValue = elementTypeOf(observed);
    final boolean reference =
        entry
            .getElement()
            .flatMap(ElementDefinition::getFhirType)
            .filter(FHIRDefinedType.REFERENCE::equals)
            .isPresent();
    final Optional<Function<Column, Column>> single =
        entry
            .primitiveType()
            .<Function<Column, Column>>map(type -> value -> converter(type).ingest(value))
            .or(
                () ->
                    structure(
                        node.elementStructure(entry).orElseThrow(),
                        (StructType) observedValue,
                        references,
                        reference));
    return entry.isRepeating()
        ? single.map(stored -> source -> functions.transform(source, stored::apply))
        : single;
  }

  /**
   * Plans the stored value of one value of a complex element, or returns empty where no element
   * beneath it is stored, in which case the element does not appear either (FR-011).
   *
   * <p>A structure the source did not carry in a given row stays absent rather than becoming a
   * structure of nulls, which is what the round trip needs of it.
   */
  @Nonnull
  private Optional<Function<Column, Column>> structure(
      @Nonnull final DefinitionCanonicalStructure node,
      @Nonnull final StructType observed,
      @Nonnull final UnaryOperator<Column> references,
      final boolean inReference) {
    final List<Function<Function<String, Column>, Column>> fields =
        node.entries().stream()
            .map(entry -> field(node, entry, observed, references, inReference))
            .flatMap(Optional::stream)
            .toList();
    if (fields.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(
        value ->
            functions.when(
                value.isNotNull(),
                functions.struct(
                    fields.stream()
                        .map(stored -> stored.apply(value::getField))
                        .toArray(Column[]::new))));
  }

  /**
   * Returns a null of the type the definitions give a primitive, as an array of it where the
   * primitive repeats.
   */
  @Nonnull
  private static Column absent(
      @Nonnull final LayoutEntry entry, @Nonnull final FHIRDefinedType type) {
    final Column value = functions.lit(null).cast(converter(type).getStored());
    return functions.when(
        functions.lit(false), entry.isRepeating() ? functions.array(value) : value);
  }

  @Nonnull
  private static PrimitiveConverter converter(@Nonnull final FHIRDefinedType type) {
    return PrimitiveConverters.forType(type)
        .orElseThrow(() -> new IllegalStateException("No converter for primitive type: " + type));
  }

  /**
   * Returns the fields of one structure of the document, in the order the layout stores them. An
   * annotation is left out, one Pathling does not define included, because annotations belong to
   * this layout and not to FHIR (decision 135). Any other field the definitions do not describe is
   * refused, or, for an element, left out.
   */
  @Nonnull
  private static Column[] egressFields(
      @Nonnull final DefinitionCanonicalStructure node,
      @Nonnull final StructType schema,
      @Nonnull final Function<String, Column> source,
      final boolean forElement) {
    return Stream.of(schema.fields())
        .filter(field -> !LayoutFields.isAnnotation(field.name()))
        .filter(field -> !forElement || node.entry(field.name()).isPresent())
        .flatMap(field -> egressField(node, field, source.apply(field.name()), forElement).stream())
        .toArray(Column[]::new);
  }

  /** Returns the value of one field of the document, or empty where it is not written at all. */
  @Nonnull
  private static Optional<Column> egressField(
      @Nonnull final DefinitionCanonicalStructure node,
      @Nonnull final StructField field,
      @Nonnull final Column stored,
      final boolean forElement) {
    final LayoutEntry entry = storedEntry(node, field);
    if (entry.isResourceType()) {
      return Optional.of(stored.alias(field.name()));
    }
    return entry.isElement()
        ? Optional.of(
            egressValue(node, entry, stored, field.dataType(), forElement).alias(field.name()))
        : Optional.empty();
  }

  /**
   * Returns the value of one element of the document, descending through an array where the element
   * repeats.
   *
   * <p>A stored structure that is null is returned as a null, because rebuilding it from its fields
   * would otherwise make it present: a structure of nulls is what Spark builds from a null, and the
   * writer would render it as an empty object. Nothing beyond that is examined. A stored structure
   * whose every field is null, and a stored array holding a null or holding nothing, pass through
   * as they are (decision 71). A repeating primitive's positional nulls are among them on purpose,
   * and so is a structure whose only content was primitive metadata (decision 71's addendum).
   */
  @Nonnull
  private static Column egressValue(
      @Nonnull final DefinitionCanonicalStructure node,
      @Nonnull final LayoutEntry entry,
      @Nonnull final Column stored,
      @Nonnull final DataType type,
      final boolean forElement) {
    if (type instanceof final ArrayType array) {
      return functions.transform(
          stored, element -> egressValue(node, entry, element, array.elementType(), forElement));
    }
    final Optional<FHIRDefinedType> primitive = entry.primitiveType();
    if (forElement && primitive.filter(FHIRDefinedType.DECIMAL::equals).isPresent()) {
      // A decimal of an element leaves as its text, marked, so that it is written as a number with
      // that text rather than through a double.
      return markedDecimal(stored);
    }
    if (primitive.isPresent()) {
      // The converter that stored the value says what the JSON writer is given for it: a decimal
      // as a double so that it is written as a bare number, and base64Binary encoded again.
      return converter(primitive.orElseThrow()).egress(stored);
    }
    final DefinitionCanonicalStructure child = node.elementStructure(entry).orElseThrow();
    return functions.when(
        stored.isNotNull(),
        functions.struct(egressFields(child, (StructType) type, stored::getField, forElement)));
  }

  /**
   * Marks the text of a decimal for {@link #unmarkDecimals}: a structure with one field, whose name
   * no FHIR element or extension can have, holding the text. A null decimal stays null.
   */
  @Nonnull
  private static Column markedDecimal(@Nonnull final Column stored) {
    return functions.when(
        stored.isNotNull(),
        functions.struct(stored.cast(DataTypes.StringType).alias(DECIMAL_MARKER)));
  }

  /**
   * Replaces each marked decimal in JSON text with a bare number of the decimal's text, so that a
   * decimal of an element is written with the text it is stored as (T100j). The match needs the
   * quotes and braces the JSON writer puts around the marker's structure, which the text of a
   * string value cannot contain unescaped, and a number's characters, so no other content of the
   * document can match it.
   *
   * @param json the JSON text, as the JSON writer wrote it
   * @return the JSON text, with every marked decimal a bare number
   */
  @Nonnull
  static Column unmarkDecimals(@Nonnull final Column json) {
    return functions.regexp_replace(json, MARKED_DECIMAL, "$1");
  }

  @Nonnull
  private static LayoutEntry storedEntry(
      @Nonnull final DefinitionCanonicalStructure node, @Nonnull final StructField field) {
    return node.entry(field.name())
        .orElseThrow(
            () ->
                new IllegalStateException(
                    "Stored field is not in the canonical structure: " + field.name()));
  }
}
