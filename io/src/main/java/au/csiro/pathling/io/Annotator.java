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
import au.csiro.pathling.schema.LayoutEntry;
import au.csiro.pathling.schema.LayoutFields;
import au.csiro.pathling.schema.MetadataGroupStructure;
import au.csiro.pathling.schema.PrimitiveTypes;
import au.csiro.pathling.schema.SchemaConfiguration;
import au.csiro.pathling.schema.annotation.AnnotationKind;
import au.csiro.pathling.schema.annotation.AnnotationRegistry;
import au.csiro.pathling.schema.annotation.AnnotationResolver;
import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Annotates resources stored in this layout: the step that stores the annotations each kind
 * computes, as a {@link SchemaConfiguration} says (decision 135, part 3).
 *
 * <p>It is a step of its own rather than part of reading, so that it applies to any input in this
 * layout, including Parquet or Delta written without annotations. Encoding runs it, and nothing
 * else does: writing stored data does not, and neither does reading for a query, because the engine
 * computes what the schema does not carry.
 *
 * <p>Each annotation goes where part 4 of the decision puts it: a primitive's in its metadata
 * group, which is added where the input has none, and a complex element's at the end of its own
 * structure. The annotations are the last fields of their container, in the order the canonical
 * structure declares, and an annotation kept from the input follows them. Every other field keeps
 * its place, and a structure in which nothing changes is passed through as it is, so resources the
 * step has nothing to do for are returned unchanged.
 *
 * <p>Whether an annotation is stored is decided from the schema, never per row. Nothing about the
 * input is checked, and what is stored for a value that does not conform is undefined, as
 * everything else about such a value is.
 */
public final class Annotator {

  @Nonnull private final DefinitionContext definitions;

  @Nonnull private final SchemaConfiguration configuration;

  @Nonnull private final AnnotationRegistry registry;

  private Annotator(
      @Nonnull final DefinitionContext definitions,
      @Nonnull final SchemaConfiguration configuration,
      @Nonnull final AnnotationRegistry registry) {
    this.definitions = definitions;
    this.configuration = configuration;
    this.registry = registry;
  }

  /**
   * Returns the step that stores the annotations Pathling defines, as a configuration says.
   *
   * @param definitions the definitions the layout is taken from
   * @param configuration which annotations to store, and what to do with those already present
   * @return the step
   */
  @Nonnull
  public static Annotator of(
      @Nonnull final DefinitionContext definitions,
      @Nonnull final SchemaConfiguration configuration) {
    return new Annotator(definitions, configuration, AnnotationRegistry.builtIn());
  }

  /**
   * Returns the step over kinds of the caller's choosing, which is how tests supply stub kinds. It
   * is not public, so that the set of kinds that reaches data stays the built-in one.
   */
  @Nonnull
  static Annotator of(
      @Nonnull final DefinitionContext definitions,
      @Nonnull final SchemaConfiguration configuration,
      @Nonnull final List<AnnotationKind> kinds) {
    return new Annotator(definitions, configuration, AnnotationRegistry.of(kinds));
  }

  /**
   * Annotates resources of one type stored in this layout.
   *
   * @param resourceType the type of the resources
   * @param resources the resources, in this layout
   * @return the resources, annotated, or the same dataset where there is nothing to change
   */
  @Nonnull
  public Dataset<Row> annotate(
      @Nonnull final String resourceType, @Nonnull final Dataset<Row> resources) {
    final DefinitionCanonicalStructure canonical =
        DefinitionCanonicalStructure.forResource(definitions, resourceType);
    return fields(canonical, resources.schema(), functions::col, Optional.empty())
        .map(columns -> resources.select(columns.toArray(Column[]::new)))
        .orElse(resources);
  }

  /**
   * Returns the fields of a structure, annotated, or empty where nothing in it changes.
   *
   * @param node the canonical structure of the node
   * @param observed the structure the input carries at this node
   * @param field how a field of the structure is reached
   * @param self the structure itself, from which a complex element's annotations are computed, or
   *     empty for a resource
   */
  @Nonnull
  private Optional<List<Column>> fields(
      @Nonnull final DefinitionCanonicalStructure node,
      @Nonnull final StructType observed,
      @Nonnull final Function<String, Column> field,
      @Nonnull final Optional<Column> self) {
    final List<Column> result = new ArrayList<>();
    boolean changed = false;
    for (final StructField stored : observed.fields()) {
      final String name = stored.name();
      if (LayoutFields.isAnnotation(name)) {
        // The annotations of a structure are placed at its end.
        continue;
      }
      final Optional<List<Column>> replaced =
          node.entry(name).flatMap(entry -> replaced(node, entry, observed, stored, field));
      changed |= replaced.isPresent();
      result.addAll(replaced.orElseGet(() -> List.of(field.apply(name).alias(name))));
    }
    final Optional<StructType> structure = Optional.of(observed);
    final Map<String, AnnotationKind> derived =
        self.isPresent() ? requested(node.entries(), structure, true) : Map.of();
    final List<String> kept = kept(node.entries(), structure, derived);
    self.ifPresent(
        value ->
            derived.forEach(
                (name, kind) ->
                    result.add(kind.apply(name, value, value, resolver(structure)).alias(name))));
    kept.forEach(name -> result.add(field.apply(name).alias(name)));
    changed |= annotationsChange(structure, derived, kept);
    return changed ? Optional.of(result) : Optional.empty();
  }

  /**
   * Returns the columns that replace one field of a structure, or empty where it does not change: a
   * complex element rebuilt, a metadata group rebuilt or left out, or a primitive followed by the
   * metadata group the input lacked.
   */
  @Nonnull
  private Optional<List<Column>> replaced(
      @Nonnull final DefinitionCanonicalStructure node,
      @Nonnull final LayoutEntry entry,
      @Nonnull final StructType observed,
      @Nonnull final StructField stored,
      @Nonnull final Function<String, Column> field) {
    final String name = stored.name();
    if (entry.isMetadataGroup()) {
      final String primitive = name.substring(LayoutFields.METADATA_GROUP_PREFIX.length());
      return metadataGroup(
          node.metadataGroup(name).orElseThrow(),
          name,
          node.entry(primitive).orElseThrow(),
          ObservedFields.find(observed, primitive).map(unused -> field.apply(primitive)),
          Optional.of(stored),
          field);
    }
    if (!entry.isElement()) {
      return Optional.empty();
    }
    if (entry.primitiveType().isPresent()) {
      final String groupName = LayoutFields.metadataGroupName(name);
      if (ObservedFields.find(observed, groupName).isPresent()) {
        // A group the input carries is rebuilt where it is met.
        return Optional.empty();
      }
      return metadataGroup(
              node.metadataGroup(groupName).orElseThrow(),
              groupName,
              entry,
              Optional.of(field.apply(name)),
              Optional.empty(),
              field)
          .map(
              group ->
                  Stream.concat(Stream.of(field.apply(name).alias(name)), group.stream()).toList());
    }
    return node.elementStructure(entry)
        .flatMap(child -> value(child, stored.dataType(), field.apply(name)))
        .map(value -> List.of(value.alias(name)));
  }

  /**
   * Returns the value of a complex element, annotated, descending through an array where it
   * repeats, or empty where nothing in it changes.
   */
  @Nonnull
  private Optional<Column> value(
      @Nonnull final DefinitionCanonicalStructure node,
      @Nonnull final DataType type,
      @Nonnull final Column stored) {
    if (type instanceof final ArrayType array) {
      final List<Column> changed = new ArrayList<>(1);
      // The function is applied as the expression is built, so whether the elements change is
      // known when it returns.
      final Column transformed =
          functions.transform(
              stored,
              each -> {
                final Optional<Column> value = value(node, array.elementType(), each);
                value.ifPresent(changed::add);
                return value.orElse(each);
              });
      return changed.isEmpty() ? Optional.empty() : Optional.of(transformed);
    }
    if (type instanceof final StructType structure) {
      return fields(node, structure, stored::getField, Optional.of(stored))
          .map(
              columns ->
                  functions.when(
                      stored.isNotNull(), functions.struct(columns.toArray(Column[]::new))));
    }
    return Optional.empty();
  }

  /**
   * Returns the metadata group of a primitive, annotated, as the columns that replace it, which are
   * none where nothing is left in it, or empty where it does not change.
   *
   * <p>Where the primitive repeats, its values and its groups are aligned by position, and the
   * group at each position is built from the value at that position. A position with neither a
   * value nor a group stays null, so that the alignment holds, and so does a singular group with
   * neither.
   *
   * <p>Where the input carries the group but not the primitive, the primitive is null in every row,
   * and an annotation is computed as it would be for a null value. That is done only for an
   * annotation the group already carries, so that no annotation that can only be null is added.
   *
   * @param node the canonical structure of the group
   * @param name the name of the group
   * @param primitive the entry of the primitive the group accompanies
   * @param value the primitive, where the input carries it
   * @param stored the group, where the input carries it
   * @param field how a field of the structure holding both is reached
   */
  @Nonnull
  private Optional<List<Column>> metadataGroup(
      @Nonnull final MetadataGroupStructure node,
      @Nonnull final String name,
      @Nonnull final LayoutEntry primitive,
      @Nonnull final Optional<Column> value,
      @Nonnull final Optional<StructField> stored,
      @Nonnull final Function<String, Column> field) {
    final boolean repeating = primitive.isRepeating();
    final Optional<StructType> observed =
        stored
            .map(StructField::dataType)
            .filter(type -> StrictnessCheck.isConformingMetadataGroup(repeating, type))
            .map(type -> (StructType) StrictnessCheck.elementTypeOf(type));
    if (stored.isPresent() && observed.isEmpty()) {
      // A group of the wrong shape is not this layout's, and is passed through as it is.
      return Optional.empty();
    }
    final Optional<Column> group = stored.map(StructField::name).map(field);
    final Map<String, AnnotationKind> derived =
        requested(node.annotationEntries(), observed, value.isPresent());
    final List<String> kept = kept(node.annotationEntries(), observed, derived);
    final boolean extensionsChange =
        observed
            .flatMap(s -> ObservedFields.find(s, LayoutFields.METADATA_GROUP_EXTENSION))
            .flatMap(
                extension ->
                    node.extensionStructure(extension.name())
                        // Whether the extensions change depends on their schema alone.
                        .flatMap(child -> value(child, extension.dataType(), functions.lit(null))))
            .isPresent();
    if (!extensionsChange && !annotationsChange(observed, derived, kept)) {
      return Optional.empty();
    }
    final boolean holdsContent = fieldNames(observed).anyMatch(n -> !LayoutFields.isAnnotation(n));
    if (!holdsContent && derived.isEmpty() && kept.isEmpty()) {
      return Optional.of(List.of());
    }
    // The value a primitive the input does not carry takes, which is the typed null of decision 72.
    final Column absent = functions.lit(null).cast(storageType(primitive));
    final Column rebuilt =
        repeating
            ? repeatingGroup(node, observed, value, group, absent, derived, kept)
            : groupAt(node, observed, value.orElse(absent), group, derived, kept);
    return Optional.of(List.of(rebuilt.alias(name)));
  }

  /**
   * Returns the group of a repeating primitive, built position by position from the value and the
   * group at each position.
   */
  @Nonnull
  private Column repeatingGroup(
      @Nonnull final MetadataGroupStructure node,
      @Nonnull final Optional<StructType> observed,
      @Nonnull final Optional<Column> values,
      @Nonnull final Optional<Column> groups,
      @Nonnull final Column absent,
      @Nonnull final Map<String, AnnotationKind> derived,
      @Nonnull final List<String> kept) {
    // Where the primitive has no values, the groups the input carries are rebuilt as they stand.
    final Optional<Column> fromGroups =
        groups.map(
            stored ->
                functions.transform(
                    stored,
                    group -> groupAt(node, observed, absent, Optional.of(group), derived, kept)));
    return values
        .map(
            stored -> {
              final Column fromValues =
                  functions.transform(
                      stored,
                      (value, index) ->
                          groupAt(
                              node,
                              observed,
                              value,
                              groups.map(group -> functions.get(group, index)),
                              derived,
                              kept));
              return fromGroups
                  .map(rebuilt -> functions.when(stored.isNotNull(), fromValues).otherwise(rebuilt))
                  .orElse(fromValues);
            })
        .orElseGet(fromGroups::orElseThrow);
  }

  /** Returns the group at one position, which is null where there is neither value nor group. */
  @Nonnull
  private Column groupAt(
      @Nonnull final MetadataGroupStructure node,
      @Nonnull final Optional<StructType> observed,
      @Nonnull final Column value,
      @Nonnull final Optional<Column> group,
      @Nonnull final Map<String, AnnotationKind> derived,
      @Nonnull final List<String> kept) {
    final Column present =
        group.map(stored -> value.isNotNull().or(stored.isNotNull())).orElseGet(value::isNotNull);
    return functions.when(
        present,
        functions.struct(
            groupFields(node, observed, value, group, derived, kept).toArray(Column[]::new)));
  }

  /**
   * Returns the fields of one metadata group: those it carries in their place, the extensions
   * annotated, then the annotations derived for it from the value beside it, and those that are
   * kept as they stand.
   */
  @Nonnull
  private List<Column> groupFields(
      @Nonnull final MetadataGroupStructure node,
      @Nonnull final Optional<StructType> observed,
      @Nonnull final Column value,
      @Nonnull final Optional<Column> group,
      @Nonnull final Map<String, AnnotationKind> derived,
      @Nonnull final List<String> kept) {
    final List<Column> result = new ArrayList<>();
    observed.ifPresent(
        structure -> {
          final Column stored = group.orElseThrow();
          for (final StructField field : structure.fields()) {
            final String name = field.name();
            if (!LayoutFields.isAnnotation(name)) {
              final Column column = stored.getField(name);
              result.add(
                  node.extensionStructure(name)
                      .flatMap(child -> value(child, field.dataType(), column))
                      .orElse(column)
                      .alias(name));
            }
          }
        });
    // The container is read only for an annotation it carries, so a group the input lacks is never
    // read.
    final Column container = group.orElseGet(() -> functions.lit(null));
    derived.forEach(
        (name, kind) ->
            result.add(kind.apply(name, value, container, resolver(observed)).alias(name)));
    kept.forEach(name -> result.add(container.getField(name).alias(name)));
    return result;
  }

  /**
   * Returns the kinds this step applies in a container, keyed by the annotation each stores there,
   * in the order the canonical structure declares: each kind this run requests, where there is a
   * value to compute it from or the container already carries the annotation.
   */
  @Nonnull
  private Map<String, AnnotationKind> requested(
      @Nonnull final List<LayoutEntry> entries,
      @Nonnull final Optional<StructType> observed,
      final boolean valuePresent) {
    final Map<String, AnnotationKind> result = new LinkedHashMap<>();
    for (final LayoutEntry entry : entries) {
      registry
          .kindFor(entry)
          .filter(kind -> kind.getSwitch().isOn(configuration))
          .filter(kind -> valuePresent || isStored(observed, entry.getName()))
          .ifPresent(kind -> result.put(entry.getName(), kind));
    }
    return result;
  }

  /**
   * Returns the annotations a container carries that are kept as they stand, in the order they are
   * carried: one of a kind this run does not request, where those are kept, and one no kind here
   * computes, unless those are dropped.
   */
  @Nonnull
  private List<String> kept(
      @Nonnull final List<LayoutEntry> entries,
      @Nonnull final Optional<StructType> observed,
      @Nonnull final Map<String, AnnotationKind> derived) {
    final Set<String> known =
        entries.stream()
            .filter(entry -> registry.kindFor(entry).isPresent())
            .map(LayoutEntry::getName)
            .collect(Collectors.toSet());
    return fieldNames(observed)
        .filter(LayoutFields::isAnnotation)
        .filter(name -> !derived.containsKey(name))
        .filter(
            name ->
                known.contains(name)
                    ? configuration.isKeepUnrequestedAnnotations()
                    : !configuration.isDropUnknownAnnotations())
        .toList();
  }

  /**
   * Returns whether the annotations of a container change: where one is computed, rather than kept
   * as the input carries it, or where one is left out or moves. A container whose annotations are
   * all kept in their place is passed through as it is.
   */
  private boolean annotationsChange(
      @Nonnull final Optional<StructType> observed,
      @Nonnull final Map<String, AnnotationKind> derived,
      @Nonnull final List<String> kept) {
    final boolean computes =
        derived.keySet().stream()
            .anyMatch(name -> configuration.isRecomputeAnnotations() || !isStored(observed, name));
    final List<String> stored = fieldNames(observed).toList();
    final List<String> rebuilt =
        Stream.of(
                stored.stream().filter(name -> !LayoutFields.isAnnotation(name)),
                derived.keySet().stream(),
                kept.stream())
            .flatMap(Function.identity())
            .toList();
    return computes || !rebuilt.equals(stored);
  }

  /**
   * Returns the resolver this step applies a kind with: the stored annotation where the container
   * the input carries has it, unless annotations are recomputed, and the computed one otherwise.
   */
  @Nonnull
  private AnnotationResolver resolver(@Nonnull final Optional<StructType> observed) {
    return (container, name, fallback) ->
        !configuration.isRecomputeAnnotations() && isStored(observed, name)
            ? container.getField(name)
            : fallback;
  }

  /** Returns whether the container the input carries has a field. */
  private static boolean isStored(
      @Nonnull final Optional<StructType> observed, @Nonnull final String name) {
    return observed.flatMap(structure -> ObservedFields.find(structure, name)).isPresent();
  }

  /** Returns the names of the fields of the container the input carries, in their order. */
  @Nonnull
  private static Stream<String> fieldNames(@Nonnull final Optional<StructType> observed) {
    return observed.stream().flatMap(structure -> Stream.of(structure.fieldNames()));
  }

  /** Returns the type a primitive is stored as. */
  @Nonnull
  private static DataType storageType(@Nonnull final LayoutEntry primitive) {
    return primitive.primitiveType().flatMap(PrimitiveTypes::storageTypeOf).orElseThrow();
  }
}
