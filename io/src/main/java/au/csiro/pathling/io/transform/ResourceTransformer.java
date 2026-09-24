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

import au.csiro.pathling.definition.DefinitionContext;
import au.csiro.pathling.schema.DefinitionCanonicalStructure;
import au.csiro.pathling.schema.LayoutEntry;
import au.csiro.pathling.schema.LayoutFields;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
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
 * is what {@link au.csiro.pathling.io.json.FhirJsonReader} and {@link
 * au.csiro.pathling.io.json.FhirJsonWriter} add. Types, cardinality, conventions and field order
 * all come from the definitions, and the schema of the input decides nothing but which elements the
 * data populates (FR-008, FR-012). The schema is fitted to the data because presence can only be
 * known after seeing it.
 *
 * <p>The input to {@link #toLayout} is in the JSON data model: every element under its FHIR JSON
 * name, a structure as a structure, a repeating element as an array, and each primitive in the type
 * Spark's JSON reader infers for it or a lossless widening of that type (see {@link
 * PrimitiveConverters}). A column of any other type contradicts the definitions and is reported
 * rather than converted.
 *
 * <p>The input carries resources of the one type it is named for. This is not checked. Where it is
 * broken, the fields of the other types are reported as undescribed content, and each row of
 * another type is stored as a resource of the named type with whatever fields the two share.
 *
 * <p>Neither direction removes what is empty, and neither checks for it (decision 71). Conformant
 * input still leaves two kinds of emptiness, both kept on purpose until M5 stores primitive
 * metadata: a structure whose only content was a primitive's id and extensions, which is an element
 * that exists and is written as an empty object whatever other conformant documents the input holds
 * (decision 72), and a repeating primitive's positional null, which is written without the metadata
 * it aligns with (decision 71's addendum). Anything else that holds nothing is assumed absent: an
 * element emptied by content the definitions do not describe or contradict, including a column
 * another row re-typed, and a structure whose every field is null in layout data from another
 * producer. Where that is broken the document written from the result is not conformant FHIR,
 * because such an element is written as an empty object rather than left out. Detecting it is
 * deferred to the JSON serde that the lexical form of a decimal also waits on.
 *
 * <p>The whole transform is dataset operations over public Spark API. No FHIR object appears in a
 * per-row plan and no expression tree is authored by hand (FR-050, FR-051).
 */
@Slf4j
public final class ResourceTransformer {

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
   * beneath it does (FR-011). What the source carries and this layout does not store is logged as a
   * warning, and {@link #findings} returns it as values.
   *
   * @param resourceType the type of the resources the source carries
   * @param source the resources, in the JSON data model
   * @return the resources, in this layout
   */
  @Nonnull
  public Dataset<Row> toLayout(
      @Nonnull final String resourceType, @Nonnull final Dataset<Row> source) {
    final StructType observed = source.schema();
    final DefinitionCanonicalStructure canonical =
        DefinitionCanonicalStructure.forResource(definitions, resourceType);
    report(StrictnessCheck.of(canonical, resourceType).check(observed));
    final Column[] columns =
        canonical.entries().stream()
            .map(entry -> resourceField(canonical, entry, observed, resourceType))
            .flatMap(Optional::stream)
            .toArray(Column[]::new);
    return source.select(columns);
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
    return stored.select(egressFields(canonical, stored.schema(), functions::col));
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
      @Nonnull final String resourceType) {
    if (entry.isResourceType()) {
      final Column column =
          observedField(observed, entry.getName())
              .map(unused -> functions.col(entry.getName()).cast(DataTypes.StringType))
              .orElseGet(() -> functions.lit(resourceType));
      return Optional.of(column.alias(entry.getName()));
    }
    return field(canonical, entry, observed).map(stored -> stored.apply(functions::col));
  }

  /**
   * Plans the column for one field of a structure, or returns empty where nothing is stored for it.
   * What is planned is a function from the way a node's fields are reached in the source to the
   * column, so that the same plan serves a field of the resource, of a structure and of each value
   * of an array.
   *
   * <p>The plan depends on the schema alone, so it is made once for every row. Annotations and
   * metadata groups are not written before M5, so neither is ever stored here. An element the
   * source did not carry does not appear. An element the source carried in a shape the definitions
   * contradict is stored as a null of the shape they declare, and never coerced: a repeating
   * element supplied as a single object would otherwise be cast to an array, and a singular element
   * supplied as an array casts to the text of that array, which is a silently wrong value rather
   * than an absence. A primitive the source carried only as its metadata group, in the shape FHIR
   * gives the group, is stored as a null of its declared type, so that the element holding it stays
   * present.
   *
   * @param node the canonical structure of the node carrying the field
   * @param entry the field
   * @param observed the structure the source was read with at this node
   */
  @Nonnull
  private Optional<Function<Function<String, Column>, Column>> field(
      @Nonnull final DefinitionCanonicalStructure node,
      @Nonnull final LayoutEntry entry,
      @Nonnull final StructType observed) {
    if (!entry.isElement()) {
      return Optional.empty();
    }
    final String name = entry.getName();
    return observedField(observed, name)
        .flatMap(
            from ->
                StrictnessCheck.shapeMismatch(node, entry, from.dataType()).isEmpty()
                    ? value(node, entry, from.dataType())
                        .<Function<Function<String, Column>, Column>>map(
                            stored -> fields -> stored.apply(fields.apply(name)))
                    : absent(node, entry, from.dataType()).map(column -> fields -> column))
        .or(() -> metadataOnly(node, entry, observed))
        .map(stored -> fields -> stored.apply(fields).alias(name));
  }

  /**
   * Plans a null of the type the definitions give a primitive the source carried only as its id and
   * extensions, or returns empty where the source carried no metadata group for it, or one in a
   * shape FHIR does not give it.
   *
   * <p>The metadata group is not stored before M5, but the element it belongs to exists, and a
   * structure holding nothing else is an element that exists too. Storing the primitive as a null
   * keeps that structure present whatever other conformant documents the input holds (decision 72).
   * For a singular primitive this is what the released encoder stores. For a repeating one it is
   * not, because the parser the released encoder reads through discards an {@code _x} array with no
   * {@code x} array beside it; the layout keeps the element, as FHIR does. The loss of the metadata
   * itself is reported as before.
   *
   * <p>A group is in the outer shape FHIR gives it where it is an object beside a singular
   * primitive and an array of objects beside a repeating one. Any other group is non-conformant,
   * and leaves the element as absent as the rest of such content leaves it.
   */
  @Nonnull
  private Optional<Function<Function<String, Column>, Column>> metadataOnly(
      @Nonnull final DefinitionCanonicalStructure node,
      @Nonnull final LayoutEntry entry,
      @Nonnull final StructType observed) {
    if (entry.primitiveType().isEmpty()) {
      return Optional.empty();
    }
    final boolean repeating = entry.isRepeating();
    return observedField(observed, LayoutFields.metadataGroupName(entry.getName()))
        .map(StructField::dataType)
        .filter(group -> StrictnessCheck.isConformingMetadataGroup(repeating, group))
        // A primitive's absent value takes its type from the definitions alone, so the group's
        // type serves as the observed one without being read.
        .flatMap(group -> absent(node, entry, group))
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
      @Nonnull final DataType observed) {
    final DataType observedValue = StrictnessCheck.elementTypeOf(observed);
    final Optional<Function<Column, Column>> single =
        entry
            .primitiveType()
            .<Function<Column, Column>>map(type -> value -> storedValue(value, type, observedValue))
            .or(
                () ->
                    structure(
                        node.elementStructure(entry).orElseThrow(), (StructType) observedValue));
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
      @Nonnull final DefinitionCanonicalStructure node, @Nonnull final StructType observed) {
    final List<Function<Function<String, Column>, Column>> fields =
        node.entries().stream()
            .map(entry -> field(node, entry, observed))
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
   * Returns a null of the type the definitions give an element whose observed shape contradicts
   * them, or empty where no such type can be built.
   *
   * <p>The type is built from columns rather than declared, so there is no second description of
   * the schema to keep in step with the walk. Where the definitions declare a structure it takes
   * the elements the source carried beneath it, typed by the definitions, and where the source
   * carried a plain value in place of a structure there is nothing to take and the element does not
   * appear. Both cases are reported as findings.
   */
  @Nonnull
  private Optional<Column> absent(
      @Nonnull final DefinitionCanonicalStructure node,
      @Nonnull final LayoutEntry entry,
      @Nonnull final DataType observed) {
    final DataType observedValue = StrictnessCheck.elementTypeOf(observed);
    final Optional<Column> single =
        entry
            .primitiveType()
            .map(ResourceTransformer::converter)
            .map(converter -> functions.lit(null).cast(converter.getStored()))
            .or(
                () ->
                    observedValue instanceof final StructType structure
                        ? absentStructure(node.elementStructure(entry).orElseThrow(), structure)
                        : Optional.empty());
    return single.map(
        value ->
            functions.when(
                functions.lit(false), entry.isRepeating() ? functions.array(value) : value));
  }

  /**
   * Returns a structure holding the elements the source carried beneath it, typed, for {@link
   * #absent} to make null.
   */
  @Nonnull
  private Optional<Column> absentStructure(
      @Nonnull final DefinitionCanonicalStructure node, @Nonnull final StructType observed) {
    final Column[] fields =
        node.entries().stream()
            .filter(LayoutEntry::isElement)
            .map(
                entry ->
                    observedField(observed, entry.getName())
                        .flatMap(from -> absent(node, entry, from.dataType()))
                        .map(column -> column.alias(entry.getName())))
            .flatMap(Optional::stream)
            .toArray(Column[]::new);
    return fields.length == 0 ? Optional.empty() : Optional.of(functions.struct(fields));
  }

  /**
   * Returns the stored value of one primitive, or a null of the stored type throughout where the
   * column's inferred type contradicts the definitions. That is decided for the column rather than
   * for the value, and the strictness check reports it.
   */
  @Nonnull
  private static Column storedValue(
      @Nonnull final Column source,
      @Nonnull final FHIRDefinedType type,
      @Nonnull final DataType observed) {
    final PrimitiveConverter converter = converter(type);
    return converter.accepts(observed)
        ? converter.ingest(source)
        : functions.lit(null).cast(converter.getStored());
  }

  @Nonnull
  private static PrimitiveConverter converter(@Nonnull final FHIRDefinedType type) {
    return PrimitiveConverters.forType(type)
        .orElseThrow(() -> new IllegalStateException("No converter for primitive type: " + type));
  }

  /**
   * Returns the content a source carries that this layout does not store, as values.
   *
   * <p>The content is ignored rather than failing the read, and returning it rather than only
   * logging it is what makes the loss detectable (decision 68).
   *
   * @param resourceType the type of the resources the source carries
   * @param observed the schema the source was read with
   * @return the findings, in the order they were reached
   */
  @Nonnull
  public List<NonConformantContent> findings(
      @Nonnull final String resourceType, @Nonnull final StructType observed) {
    return StrictnessCheck.of(
            DefinitionCanonicalStructure.forResource(definitions, resourceType), resourceType)
        .check(observed);
  }

  /** Logs the content this layout does not store, which is ignored rather than failing the read. */
  private static void report(@Nonnull final List<NonConformantContent> findings) {
    if (findings.isEmpty()) {
      return;
    }
    final String detail =
        findings.stream().map(NonConformantContent::toString).collect(Collectors.joining("; "));
    // Ignoring is the only mode, and ignoring is not silence.
    log.warn("Ignoring content that this layout does not store: {}", detail);
  }

  @Nonnull
  private static Optional<StructField> observedField(
      @Nonnull final StructType observed, @Nonnull final String name) {
    return Stream.of(observed.fields()).filter(field -> field.name().equals(name)).findFirst();
  }

  /** Returns the fields of one structure of the document, in the order the layout stores them. */
  @Nonnull
  private static Column[] egressFields(
      @Nonnull final DefinitionCanonicalStructure node,
      @Nonnull final StructType schema,
      @Nonnull final Function<String, Column> source) {
    return Stream.of(schema.fields())
        .flatMap(field -> egressField(node, field, source.apply(field.name())).stream())
        .toArray(Column[]::new);
  }

  /** Returns the value of one field of the document, or empty where it is not written at all. */
  @Nonnull
  private static Optional<Column> egressField(
      @Nonnull final DefinitionCanonicalStructure node,
      @Nonnull final StructField field,
      @Nonnull final Column stored) {
    final LayoutEntry entry = storedEntry(node, field);
    if (entry.isResourceType()) {
      return Optional.of(stored.alias(field.name()));
    }
    return entry.isElement()
        ? Optional.of(egressValue(node, entry, stored, field.dataType()).alias(field.name()))
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
      @Nonnull final DataType type) {
    if (type instanceof final ArrayType array) {
      return functions.transform(
          stored, element -> egressValue(node, entry, element, array.elementType()));
    }
    final Optional<FHIRDefinedType> primitive = entry.primitiveType();
    if (primitive.isPresent()) {
      // The converter that stored the value says what the JSON writer is given for it: a decimal
      // as a double so that it is written as a bare number, and base64Binary encoded again.
      return converter(primitive.orElseThrow()).egress(stored);
    }
    final DefinitionCanonicalStructure child = node.elementStructure(entry).orElseThrow();
    return functions.when(
        stored.isNotNull(),
        functions.struct(egressFields(child, (StructType) type, stored::getField)));
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
