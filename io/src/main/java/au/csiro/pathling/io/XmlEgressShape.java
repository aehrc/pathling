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

import au.csiro.pathling.definition.ElementDefinition;
import au.csiro.pathling.schema.DefinitionCanonicalStructure;
import au.csiro.pathling.schema.LayoutEntry;
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
 * Shapes resources stored in this layout into the structure Spark's XML writer writes as FHIR XML
 * (ticket 32).
 *
 * <p>A primitive's value is written as its {@code value} attribute, and the id of an element and
 * the url of an extension as attributes too, by naming each with the underscore that marks an
 * attribute. Fields are written in the order they are stored, which is the order of the
 * definitions. What the stored layout holds beside FHIR's elements, its annotations and the groups
 * of a primitive's metadata, is not written, as it is not written to JSON.
 *
 * <p>What is written matches the previous writer, which converted the JSON writer's output with the
 * FHIR parser (decision 84), but for a decimal, which is written as the text it is stored as, so
 * that XML keeps its lexical form from end to end (owner, 2026-10-22). A null in an array is left
 * out, so a repeating primitive's positional null is not written; base64Binary is encoded without
 * line breaks; and a structure that holds nothing is left out, which the writer leaves to {@link
 * XmlDocuments#finish}. A narrative is written as the text of its {@code div}, which {@link
 * XmlDocuments#finish} replaces with the markup the text holds.
 */
final class XmlEgressShape {

  /** The prefix Spark's XML writer writes a field as an attribute for. */
  @Nonnull private static final String ATTRIBUTE = "_";

  private XmlEgressShape() {}

  /**
   * Returns the structure the XML writer writes a stored resource as.
   *
   * @param resource the canonical structure of the resource's type
   * @param stored the schema of the stored resources
   * @return the structure, with the FHIR namespace as an attribute of the root
   */
  @Nonnull
  static Column shape(
      @Nonnull final DefinitionCanonicalStructure resource, @Nonnull final StructType stored) {
    final List<Column> fields = new ArrayList<>();
    fields.add(functions.lit(XmlDocuments.FHIR_NAMESPACE).alias(ATTRIBUTE + "xmlns"));
    fields.addAll(fields(resource, null, true, stored, functions::col));
    return functions.struct(fields.toArray(Column[]::new));
  }

  @Nonnull
  private static List<Column> fields(
      @Nonnull final DefinitionCanonicalStructure node,
      @Nullable final FHIRDefinedType nodeType,
      final boolean isResource,
      @Nonnull final StructType stored,
      @Nonnull final Function<String, Column> source) {
    final List<Column> columns = new ArrayList<>();
    for (final StructField field : stored.fields()) {
      final Optional<LayoutEntry> found = node.entry(field.name());
      if (found.isEmpty() || !found.get().isElement()) {
        continue;
      }
      final LayoutEntry entry = found.get();
      final String name = entry.getName();
      final Column column = source.apply(name);
      final Optional<FHIRDefinedType> primitive = entry.primitiveType();
      if (XmlIngestShape.isAttribute(nodeType, isResource, name)) {
        columns.add(column.cast(DataTypes.StringType).alias(ATTRIBUTE + name));
      } else if (primitive.filter(FHIRDefinedType.XHTML::equals).isPresent()) {
        // The narrative is written as the text of its div, which XmlDocuments.finish replaces with
        // the markup it holds.
        columns.add(column.alias(name));
      } else if (primitive.isPresent()) {
        final FHIRDefinedType type = primitive.get();
        columns.add(
            each(
                    field.dataType(),
                    column,
                    value ->
                        functions.when(
                            value.isNotNull(),
                            functions.struct(text(type, value).alias(ATTRIBUTE + "value"))))
                .alias(name));
      } else {
        final DefinitionCanonicalStructure child = node.elementStructure(entry).orElseThrow();
        final StructType inner = (StructType) elementTypeOf(field.dataType());
        final FHIRDefinedType childType =
            entry.getElement().flatMap(ElementDefinition::getFhirType).orElse(null);
        if (hasElements(child, inner)) {
          columns.add(
              each(
                      field.dataType(),
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
    }
    return columns;
  }

  /** Returns whether a stored structure has any field that is written. */
  private static boolean hasElements(
      @Nonnull final DefinitionCanonicalStructure node, @Nonnull final StructType stored) {
    return Stream.of(stored.fields())
        .anyMatch(field -> node.entry(field.name()).filter(LayoutEntry::isElement).isPresent());
  }

  /** Applies a plan to a value, or to each value of an array, leaving the nulls of an array out. */
  @Nonnull
  private static Column each(
      @Nonnull final DataType stored,
      @Nonnull final Column column,
      @Nonnull final UnaryOperator<Column> single) {
    if (stored instanceof ArrayType) {
      return functions.filter(functions.transform(column, single::apply), Column::isNotNull);
    }
    return single.apply(column);
  }

  /** Returns a primitive's value as the text of its {@code value} attribute. */
  @Nonnull
  private static Column text(@Nonnull final FHIRDefinedType type, @Nonnull final Column value) {
    return switch (type) {
      case BOOLEAN ->
          functions.when(value, functions.lit("true")).otherwise(functions.lit("false"));
      case BASE64BINARY -> functions.regexp_replace(functions.base64(value), "[\\r\\n]", "");
      default -> value.cast(DataTypes.StringType);
    };
  }
}
