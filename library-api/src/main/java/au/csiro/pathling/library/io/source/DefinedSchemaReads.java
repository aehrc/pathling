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

package au.csiro.pathling.library.io.source;

import au.csiro.pathling.definition.fhir.FhirDefinitionContext;
import au.csiro.pathling.io.FhirReader;
import au.csiro.pathling.io.Projection;
import au.csiro.pathling.io.SchemaSet;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.utilities.AnnotationNames;
import jakarta.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Reads the resource types a schema set names with their schemas, for the sources of this package.
 * A type the set does not name is read as it is without a set.
 *
 * <p>Stored data, which already has a schema, is projected onto the layout schema of the set's
 * schema and then annotated as the configuration says, and the stored paths the projection drops
 * are logged at debug level (FR-006a, FR-007). FHIR text is read with the io defined-schema read,
 * which shapes it once, and is annotated in the same way.
 */
@Slf4j
final class DefinedSchemaReads {

  @Nonnull private final PathlingContext context;

  @Nonnull private final SchemaSet schemaSet;

  private DefinedSchemaReads(
      @Nonnull final PathlingContext context, @Nonnull final SchemaSet schemaSet) {
    this.context = context;
    this.schemaSet = schemaSet;
  }

  /**
   * Returns the reads of a context under a schema set.
   *
   * @param context the Pathling context
   * @param schemaSet the schema set, which may be empty
   * @return the reads
   */
  @Nonnull
  static DefinedSchemaReads of(
      @Nonnull final PathlingContext context, @Nonnull final SchemaSet schemaSet) {
    return new DefinedSchemaReads(context, schemaSet);
  }

  /**
   * Returns the empty schema set, which is the same as no set.
   *
   * @return the empty set
   */
  @Nonnull
  static SchemaSet noSchemas() {
    return SchemaSet.of(Map.of());
  }

  /**
   * Presents the stored data of a resource type under the schema the set gives it, annotated as the
   * configuration says, or returns it unchanged if the set does not name the type.
   *
   * <p>Fields the schema lacks are dropped, and logged; fields the stored data lacks are null. Only
   * the two schemas are compared, so this runs no Spark job.
   *
   * @param stored the stored data
   * @param resourceType the resource type
   * @return the data under the set's schema
   * @throws IllegalArgumentException if a stored field differs from a field of the schema only in
   *     case, or a stored struct holds two fields differing only in case, naming the resource type
   *     and both paths
   */
  @Nonnull
  Dataset<Row> project(@Nonnull final Dataset<Row> stored, @Nonnull final String resourceType) {
    return schemaSet
        .schema(resourceType)
        .map(
            schema -> {
              final StructType target = layoutSchema(resourceType, schema);
              final Dataset<Row> projected =
                  refusedFor(resourceType, () -> Projection.project(stored, target));
              logDroppedPaths(resourceType, stored.schema(), target);
              return context.annotate(projected, resourceType);
            })
        .orElse(stored);
  }

  /**
   * Encodes NDJSON lines of FHIR JSON, one resource per line, as the resources of a type: with the
   * io defined-schema read where the set names the type, and as {@link PathlingContext#encode} does
   * otherwise.
   *
   * @param lines the lines, in a dataset of one string column
   * @param resourceType the resource type
   * @return the encoded resources
   */
  @Nonnull
  Dataset<Row> encode(@Nonnull final Dataset<Row> lines, @Nonnull final String resourceType) {
    return schemaSet
        .schema(resourceType)
        .map(
            schema -> {
              final Dataset<String> documents = lines.as(Encoders.STRING());
              // The documents of the type are selected as encoding selects them, keeping a document
              // whose type cannot be read so that text which is not JSON still fails the read.
              final Column type =
                  functions.get_json_object(
                      documents.col(documents.columns()[0]), "$.resourceType");
              // A line of only whitespace holds no resource, so it is dropped as encoding drops it,
              // rather than being read as a resource with every element absent.
              final Column line = documents.col(documents.columns()[0]);
              final Dataset<String> selected =
                  documents.filter(
                      line.rlike("\\S").and(type.isNull().or(type.equalTo(resourceType))));
              return context.annotate(
                  reader().json().read(resourceType, selected, schema), resourceType);
            })
        .orElseGet(() -> context.encode(lines, resourceType, PathlingContext.FHIR_JSON));
  }

  /**
   * Encodes the resources of a type from FHIR bundles: with the io defined-schema read where the
   * set names the type, and as {@link PathlingContext#encodeBundle} does otherwise.
   *
   * @param bundles the bundles, in a dataset of one string column
   * @param resourceType the resource type
   * @param mimeType the media type of the bundles
   * @return the encoded resources
   */
  @Nonnull
  Dataset<Row> encodeBundles(
      @Nonnull final Dataset<Row> bundles,
      @Nonnull final String resourceType,
      @Nonnull final String mimeType) {
    return schemaSet
        .schema(resourceType)
        .map(
            schema ->
                context.annotate(
                    reader()
                        .format(mimeType)
                        .readBundles(resourceType, bundles.as(Encoders.STRING()), schema),
                    resourceType))
        .orElseGet(() -> context.encodeBundle(bundles, resourceType, mimeType));
  }

  @Nonnull
  private FhirReader reader() {
    return FhirReader.of(
        context.getSpark(),
        FhirDefinitionContext.of(context.getFhirContext()),
        context.getSchemaConfiguration());
  }

  /**
   * Returns the layout schema of a defined schema: the schema the io defined-schema read gives it,
   * taken from the read of no documents, which analyses a plan and runs no job.
   */
  @Nonnull
  private StructType layoutSchema(
      @Nonnull final String resourceType, @Nonnull final StructType schema) {
    final Dataset<String> none =
        context.getSpark().createDataset(List.<String>of(), Encoders.STRING());
    return reader().json().read(resourceType, none, schema).schema();
  }

  /** Runs a read, naming the resource type in the error it is refused with. */
  @Nonnull
  private static Dataset<Row> refusedFor(
      @Nonnull final String resourceType, @Nonnull final Supplier<Dataset<Row>> read) {
    try {
      return read.get();
    } catch (final IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Cannot read " + resourceType + " with its schema: " + e.getMessage(), e);
    }
  }

  /**
   * Logs at debug level the paths of a stored schema that a target schema drops, other than
   * annotations. A dropped struct is logged as one path, not field by field.
   */
  private static void logDroppedPaths(
      @Nonnull final String resourceType,
      @Nonnull final StructType stored,
      @Nonnull final StructType target) {
    if (log.isDebugEnabled()) {
      final List<String> dropped = droppedPaths(stored, target, "").toList();
      if (!dropped.isEmpty()) {
        log.debug(
            "Reading {} with its schema drops these stored paths: {}",
            resourceType,
            dropped.stream().map(path -> "`" + path + "`").collect(Collectors.joining(", ")));
      }
    }
  }

  @Nonnull
  private static Stream<String> droppedPaths(
      @Nonnull final DataType stored,
      @Nonnull final DataType target,
      @Nonnull final String prefix) {
    if (stored instanceof final StructType storedStruct
        && target instanceof final StructType targetStruct) {
      return Arrays.stream(storedStruct.fields())
          .filter(field -> !AnnotationNames.isAnnotation(field.name()))
          .flatMap(field -> droppedPaths(field, targetStruct, prefix));
    } else if (stored instanceof final ArrayType storedArray
        && target instanceof final ArrayType targetArray) {
      return droppedPaths(storedArray.elementType(), targetArray.elementType(), prefix);
    } else if (stored instanceof final MapType storedMap
        && target instanceof final MapType targetMap) {
      return droppedPaths(storedMap.valueType(), targetMap.valueType(), prefix);
    }
    return Stream.empty();
  }

  @Nonnull
  private static Stream<String> droppedPaths(
      @Nonnull final StructField field,
      @Nonnull final StructType target,
      @Nonnull final String prefix) {
    final String path = prefix + field.name();
    return Arrays.stream(target.fields())
        .filter(targetField -> targetField.name().equals(field.name()))
        .findFirst()
        .map(targetField -> droppedPaths(field.dataType(), targetField.dataType(), path + "."))
        .orElseGet(() -> Stream.of(path));
  }
}
