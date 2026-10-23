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

import au.csiro.pathling.definition.ElementDefinition;
import au.csiro.pathling.schema.DefinitionCanonicalStructure;
import au.csiro.pathling.schema.LayoutEntry;
import au.csiro.pathling.schema.LayoutFields;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Shapes FHIR XML, as Spark's XML reader parses it, into the JSON data model, so that it reaches
 * this layout through the same transform as JSON (ticket 32, FR-060, decision 134).
 *
 * <p>The shape is planned from two things: the definitions, which give every element its type,
 * cardinality and place, and the schema Spark's XML reader inferred from the documents, every value
 * read as text, which says which elements are present. Spark reads XML into its own shape: a
 * primitive's value, an {@code Element.id} and an {@code Extension.url} are attributes, read as
 * fields prefixed with an underscore; a repeating element that occurs once in every document is a
 * single value; and a primitive's id and extensions are inside it rather than in a group beside it.
 * The plan undoes each of these.
 *
 * <p>A value is typed from the definitions alone. A boolean is a boolean where its text is {@code
 * true} or {@code false}, and an integer is read as a long. A decimal is stored as the text it is,
 * so XML keeps a decimal's lexical form where JSON passes it through a double (owner, 2026-10-22).
 *
 * <p>What the definitions do not describe is left out without a finding, as ingest in {@code trust}
 * may (owner, 2026-10-22): an element, an attribute, text inside an element, and an attribute in a
 * namespace such as {@code xsi:schemaLocation}. So is {@code contained}, which is valid FHIR that
 * the layout does not store (FR-006).
 */
final class XmlIngestShape {

  /** The prefix that marks a field as an attribute, to Spark's XML reader and writer alike. */
  @Nonnull static final String ATTRIBUTE = "_";

  /** The field a primitive's value is read into, from its {@code value} attribute. */
  @Nonnull private static final String VALUE = "_value";

  /** The field a primitive's id is read into, from its {@code id} attribute. */
  @Nonnull private static final String ID = "_id";

  @Nonnull private static final String EXTENSION = "extension";

  @Nonnull private final DefinitionCanonicalStructure resource;

  @Nonnull private final String resourceType;

  @Nonnull private final StructType parseSchema;

  private XmlIngestShape(
      @Nonnull final DefinitionCanonicalStructure resource,
      @Nonnull final String resourceType,
      @Nonnull final StructType parseSchema) {
    this.resource = resource;
    this.resourceType = resourceType;
    this.parseSchema = parseSchema;
  }

  /**
   * Plans the shape of the resources of one type, from the schema Spark's XML reader inferred from
   * them.
   *
   * @param resource the canonical structure of the type
   * @param resourceType the type
   * @param inferred the schema inferred from the documents
   * @return the plan
   */
  @Nonnull
  static XmlIngestShape of(
      @Nonnull final DefinitionCanonicalStructure resource,
      @Nonnull final String resourceType,
      @Nonnull final StructType inferred) {
    return new XmlIngestShape(
        resource, resourceType, parseStructure(resource, null, true, inferred));
  }

  /**
   * Returns the schema the documents are parsed with: what the definitions describe, every value as
   * text.
   *
   * @return the schema
   */
  @Nonnull
  StructType parseSchema() {
    return parseSchema;
  }

  /**
   * Returns one resource in the JSON data model.
   *
   * @param parsed the document, parsed with {@link #parseSchema()}, or null where the schema has no
   *     fields and nothing was parsed
   * @param document the document, which is null only where the row holds no resource
   * @return the resource, as a structure
   */
  @Nonnull
  Column shape(@Nullable final Column parsed, @Nonnull final Column document) {
    final List<Column> columns = new ArrayList<>();
    // Nullable, as the JSON reader's column is, so that the stored schemas are equal.
    columns.add(
        functions
            .when(document.isNotNull(), functions.lit(resourceType))
            .alias(LayoutFields.RESOURCE_TYPE));
    if (parsed != null) {
      columns.addAll(fields(resource, null, true, parseSchema, parsed::getField));
    }
    return functions.struct(columns.toArray(Column[]::new));
  }

  /** Plans the parse schema of one structure, keeping only what the definitions describe. */
  @Nonnull
  private static StructType parseStructure(
      @Nonnull final DefinitionCanonicalStructure node,
      @Nullable final FHIRDefinedType nodeType,
      final boolean isResource,
      @Nonnull final StructType observed) {
    final List<StructField> kept = new ArrayList<>();
    for (final StructField field : observed.fields()) {
      final String name = field.name();
      if (name.startsWith(ATTRIBUTE)) {
        if (isAttribute(nodeType, isResource, name.substring(ATTRIBUTE.length()))) {
          kept.add(text(name));
        }
      } else {
        element(node, nodeType, isResource, name)
            .flatMap(entry -> parseElement(node, entry, field.dataType()))
            .ifPresent(type -> kept.add(DataTypes.createStructField(name, type, true)));
      }
    }
    return DataTypes.createStructType(kept);
  }

  /**
   * Plans the parse type of an element the definitions describe, or returns empty where it carries
   * nothing they describe, such as text alone.
   */
  @Nonnull
  private static Optional<DataType> parseElement(
      @Nonnull final DefinitionCanonicalStructure node,
      @Nonnull final LayoutEntry entry,
      @Nonnull final DataType observed) {
    if (!(elementTypeOf(observed) instanceof final StructType structure)) {
      return Optional.empty();
    }
    final StructType parsed =
        entry.primitiveType().isPresent()
            ? DataTypes.createStructType(
                Stream.of(structure.fields())
                    .map(StructField::name)
                    .filter(name -> name.equals(VALUE) || name.equals(ID) || name.equals(EXTENSION))
                    .map(XmlIngestShape::text)
                    .toList())
            : parseStructure(
                node.elementStructure(entry).orElseThrow(), fhirType(entry), false, structure);
    return parsed.fields().length == 0 ? Optional.empty() : Optional.of(rewrap(observed, parsed));
  }

  /**
   * Returns the fields of one structure in the JSON data model, in the order of the definitions.
   */
  @Nonnull
  private List<Column> fields(
      @Nonnull final DefinitionCanonicalStructure node,
      @Nullable final FHIRDefinedType nodeType,
      final boolean isResource,
      @Nonnull final StructType parsed,
      @Nonnull final Function<String, Column> source) {
    final List<Column> columns = new ArrayList<>();
    for (final LayoutEntry entry : node.entries()) {
      if (!entry.isElement()) {
        continue;
      }
      final String name = entry.getName();
      if (isAttribute(nodeType, isResource, name)) {
        if (fieldOf(parsed, ATTRIBUTE + name).isPresent()) {
          columns.add(source.apply(ATTRIBUTE + name).alias(name));
        }
        continue;
      }
      final Optional<StructField> field = fieldOf(parsed, name);
      if (field.isEmpty()) {
        continue;
      }
      final DataType parsedType = field.get().dataType();
      final Column column = source.apply(name);
      if (entry.primitiveType().isPresent()) {
        columns.addAll(primitive(entry, parsedType, column));
      } else {
        final DefinitionCanonicalStructure child = node.elementStructure(entry).orElseThrow();
        final StructType inner = (StructType) elementTypeOf(parsedType);
        final FHIRDefinedType childType = fhirType(entry);
        columns.add(
            wrap(
                    parsedType,
                    entry.isRepeating(),
                    column,
                    value ->
                        functions.when(
                            value.isNotNull(),
                            functions.struct(
                                fields(child, childType, false, inner, value::getField)
                                    .toArray(Column[]::new))))
                .alias(name));
      }
    }
    return columns;
  }

  /**
   * Returns the value of a primitive, and the group holding its id and extensions where any are
   * present, in the JSON data model.
   */
  @Nonnull
  private static List<Column> primitive(
      @Nonnull final LayoutEntry entry,
      @Nonnull final DataType parsedType,
      @Nonnull final Column column) {
    final StructType parsed = (StructType) elementTypeOf(parsedType);
    final List<Column> columns = new ArrayList<>();
    final boolean repeating = entry.isRepeating();
    if (fieldOf(parsed, VALUE).isPresent()) {
      final FHIRDefinedType type = entry.primitiveType().orElseThrow();
      columns.add(
          wrap(parsedType, repeating, column, value -> typed(type, value.getField(VALUE)))
              .alias(entry.getName()));
    }
    final List<String> metadata =
        Stream.of(ID, EXTENSION).filter(name -> fieldOf(parsed, name).isPresent()).toList();
    if (!metadata.isEmpty()) {
      final UnaryOperator<Column> group =
          value -> {
            final Column[] fields =
                metadata.stream()
                    .map(name -> value.getField(name).alias(name.equals(ID) ? "id" : name))
                    .toArray(Column[]::new);
            final Column present =
                metadata.stream()
                    .map(name -> value.getField(name).isNotNull())
                    .reduce(Column::or)
                    .orElseThrow();
            return functions.when(present, functions.struct(fields));
          };
      columns.add(
          wrap(parsedType, repeating, column, group)
              .alias(LayoutFields.metadataGroupName(entry.getName())));
    }
    return columns;
  }

  /**
   * Returns a primitive's value typed from the definitions: a boolean where its text is {@code
   * true} or {@code false}, an integer as a long, and anything else, a decimal included, as its
   * text.
   */
  @Nonnull
  private static Column typed(@Nonnull final FHIRDefinedType type, @Nonnull final Column text) {
    return switch (type) {
      case BOOLEAN ->
          functions
              .when(text.equalTo("true"), functions.lit(true))
              .when(text.equalTo("false"), functions.lit(false));
      case INTEGER, UNSIGNEDINT, POSITIVEINT -> text.try_cast(DataTypes.LongType);
      default -> text;
    };
  }

  /**
   * Applies a plan to each occurrence of an element, and makes a single occurrence of a repeating
   * one an array.
   */
  @Nonnull
  private static Column wrap(
      @Nonnull final DataType parsed,
      final boolean repeating,
      @Nonnull final Column column,
      @Nonnull final UnaryOperator<Column> single) {
    if (parsed instanceof ArrayType) {
      return functions.transform(column, single::apply);
    }
    return repeating
        ? functions.when(column.isNotNull(), functions.array(single.apply(column)))
        : single.apply(column);
  }

  /**
   * Returns the entry of an element the definitions describe at a node, other than one carried as
   * an attribute.
   */
  @Nonnull
  private static Optional<LayoutEntry> element(
      @Nonnull final DefinitionCanonicalStructure node,
      @Nullable final FHIRDefinedType nodeType,
      final boolean isResource,
      @Nonnull final String name) {
    return node.entry(name)
        .filter(LayoutEntry::isElement)
        .filter(entry -> !isAttribute(nodeType, isResource, name));
  }

  /**
   * Returns whether FHIR XML carries an element as an attribute: the id of anything but a resource,
   * and the url of an extension. Writing follows the same rule, so that what is written is read
   * back.
   *
   * @param nodeType the FHIR type of the element's parent, where it has one
   * @param isResource whether the parent is a resource
   * @param name the name of the element
   * @return whether the element is an attribute
   */
  static boolean isAttribute(
      @Nullable final FHIRDefinedType nodeType,
      final boolean isResource,
      @Nonnull final String name) {
    return ("id".equals(name) && !isResource)
        || ("url".equals(name) && FHIRDefinedType.EXTENSION.equals(nodeType));
  }

  /** Returns the FHIR type the definitions give an entry, or null where they give none. */
  @Nullable
  static FHIRDefinedType fhirType(@Nonnull final LayoutEntry entry) {
    return entry.getElement().flatMap(ElementDefinition::getFhirType).orElse(null);
  }

  @Nonnull
  private static StructField text(@Nonnull final String name) {
    return DataTypes.createStructField(name, DataTypes.StringType, true);
  }

  @Nonnull
  private static DataType rewrap(@Nonnull final DataType observed, @Nonnull final DataType inner) {
    return observed instanceof ArrayType ? DataTypes.createArrayType(inner, true) : inner;
  }
}
