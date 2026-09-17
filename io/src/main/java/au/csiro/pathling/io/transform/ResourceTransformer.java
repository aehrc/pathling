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
import au.csiro.pathling.definition.ElementDefinition;
import au.csiro.pathling.definition.FhirType;
import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.schema.DefinitionCanonicalStructure;
import au.csiro.pathling.schema.LayoutEntry;
import au.csiro.pathling.schema.LayoutFields;
import au.csiro.pathling.schema.PrimitiveTypes;
import au.csiro.pathling.schema.SchemaBuilder;
import au.csiro.pathling.schema.SchemaConfiguration;
import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Reads FHIR JSON and transforms it into the definition-derived schema (R-008).
 *
 * <p>The source is read with an inferred schema and then imposed upon: types, cardinality,
 * conventions and field order all come from the definitions, and the inferred schema decides
 * nothing but which elements the data populates (FR-008, FR-012). Inference is used because the
 * schema is fitted to the data and presence can only be known after seeing it; reading with the
 * derived schema instead would need a discovery pass of its own for the same answer.
 *
 * <p>The whole transform is dataset operations over public Spark API. No FHIR object appears in a
 * per-row plan and no expression tree is authored by hand (FR-050).
 */
@Slf4j
public final class ResourceTransformer {

  /** The reader option that decides what happens to a document that is not valid JSON. */
  @Nonnull private static final String READ_MODE = "mode";

  /**
   * Content that is not JSON at all is outside the strictness switch, which governs content the
   * definitions do not describe. Reading it leniently would yield a row of nulls in place of a
   * resource, which is the silent truncation FR-018 forbids.
   */
  @Nonnull private static final String FAIL_FAST = "FAILFAST";

  @Nonnull private final DefinitionContext definitions;

  @Nonnull private final SchemaConfiguration configuration;

  @Nonnull private final SchemaBuilder schemaBuilder;

  private ResourceTransformer(
      @Nonnull final DefinitionContext definitions,
      @Nonnull final SchemaConfiguration configuration,
      @Nonnull final SchemaBuilder schemaBuilder) {
    this.definitions = definitions;
    this.configuration = configuration;
    this.schemaBuilder = schemaBuilder;
  }

  /**
   * Returns a transformer over a set of definitions.
   *
   * @param definitions the definitions the schema is derived from
   * @param configuration the configuration of the schema mode and the strictness switch
   * @param maxNestingLevel how many times a type may recur within itself, in the dense mode
   * @param enableExtensions whether extensions are carried, in the dense mode
   * @param enabledOpenTypes the types an open choice expands to, in the dense mode
   * @return the transformer
   */
  @Nonnull
  public static ResourceTransformer of(
      @Nonnull final DefinitionContext definitions,
      @Nonnull final SchemaConfiguration configuration,
      final int maxNestingLevel,
      final boolean enableExtensions,
      @Nonnull final Set<String> enabledOpenTypes) {
    return new ResourceTransformer(
        definitions,
        configuration,
        SchemaBuilder.of(definitions, maxNestingLevel, enableExtensions, enabledOpenTypes));
  }

  /**
   * Reads newline-delimited FHIR JSON and transforms it into the derived schema.
   *
   * <p>This is the ingest path on which the lexical form of a number survives. Reading resources as
   * a dataset of strings routes every number through a double, which is a defect in the reader
   * rather than in this layout, and is documented per path (FR-020).
   *
   * @param spark the Spark session to read with
   * @param resourceType the type of the resources the source carries
   * @param path the path to read from
   * @return the transformed dataset
   */
  @Nonnull
  public Dataset<Row> read(
      @Nonnull final SparkSession spark,
      @Nonnull final String resourceType,
      @Nonnull final String path) {
    return transform(resourceType, source(spark, path));
  }

  /**
   * Reads newline-delimited FHIR JSON with an inferred schema, which is what the transform is
   * imposed upon.
   *
   * <p>It is exposed so that a caller can ask what a source carries before deciding to store it:
   * {@link #findings} takes the schema this returns.
   *
   * @param spark the Spark session to read with
   * @param path the path to read from
   * @return the source, read with an inferred schema
   */
  @Nonnull
  public Dataset<Row> source(@Nonnull final SparkSession spark, @Nonnull final String path) {
    return spark
        .read()
        .options(DecimalTransform.lexicalReadOptions())
        .option(READ_MODE, FAIL_FAST)
        .json(path);
  }

  /**
   * Transforms a source read with an inferred schema into the derived schema.
   *
   * @param resourceType the type of the resources the source carries
   * @param source the source, read with an inferred schema
   * @return the transformed dataset
   * @throws InvalidUserInputError where the source carries content this layout does not store and
   *     the strictness switch is set to fail
   */
  @Nonnull
  public Dataset<Row> transform(
      @Nonnull final String resourceType, @Nonnull final Dataset<Row> source) {
    final StructType observed = source.schema();
    final DefinitionCanonicalStructure canonical =
        DefinitionCanonicalStructure.forResource(definitions, resourceType);
    report(findings(resourceType, observed));
    final StructType target =
        configuration.isDenseSchema()
            ? schemaBuilder.dense(resourceType)
            : schemaBuilder.pruned(resourceType, stored(observed));
    final Column[] columns =
        Stream.of(target.fields())
            .map(field -> resourceTypeOrElement(canonical, field, observed, resourceType))
            .toArray(Column[]::new);
    return source.select(columns);
  }

  /**
   * Returns the column for one field of a resource. The field naming the resource type has no
   * element behind it, so it is the one field taken from the source without consulting a
   * definition, and it falls back to the type the caller named where a source omits it.
   */
  @Nonnull
  private Column resourceTypeOrElement(
      @Nonnull final DefinitionCanonicalStructure canonical,
      @Nonnull final StructField field,
      @Nonnull final StructType observed,
      @Nonnull final String resourceType) {
    if (LayoutFields.RESOURCE_TYPE.equals(field.name())) {
      return observedField(observed, field.name())
          .map(unused -> functions.col(field.name()).cast(field.dataType()))
          .orElseGet(() -> functions.lit(resourceType))
          .alias(field.name());
    }
    return column(canonical, field, observed, functions::col, resourceType).alias(field.name());
  }

  /**
   * Returns the column for one field of a structure, which is a null of the field's own type
   * wherever the source has nothing to put there.
   *
   * <p>Three things arrive at that null. An element the source did not carry, which arises in the
   * dense mode and is what an absent element means. A metadata group, which the derivation provides
   * and this transform does not yet populate. And content whose shape contradicts the definitions,
   * which is never coerced: a repeating element supplied as a single object would otherwise be cast
   * to an array, and a singular element supplied as an array casts to the text of that array, which
   * is a silently wrong value rather than a failure.
   *
   * <p>The path is that of the structure carrying the field, and is carried down only so that a
   * value that contradicts its declared type can be named.
   */
  @Nonnull
  private Column column(
      @Nonnull final DefinitionCanonicalStructure canonical,
      @Nonnull final StructField field,
      @Nonnull final StructType observed,
      @Nonnull final Function<String, Column> source,
      @Nonnull final String path) {
    final LayoutEntry entry =
        canonical
            .entry(field.name())
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Derived field is not in the canonical structure: " + field.name()));
    return observedField(observed, field.name())
        .filter(from -> !entry.isMetadataGroup())
        .filter(from -> shapeMatches(field.dataType(), from.dataType()))
        .map(
            from ->
                value(
                    canonical,
                    entry,
                    source.apply(field.name()),
                    field.dataType(),
                    from.dataType(),
                    path + "." + field.name()))
        .orElseGet(() -> functions.lit(null).cast(field.dataType()));
  }

  /** Returns the stored value of an element the source carried in the shape it declares. */
  @Nonnull
  private Column value(
      @Nonnull final DefinitionCanonicalStructure canonical,
      @Nonnull final LayoutEntry entry,
      @Nonnull final Column source,
      @Nonnull final DataType target,
      @Nonnull final DataType observed,
      @Nonnull final String path) {
    final ElementDefinition element = entry.getElement().orElseThrow();
    final FhirType type = element.getFhirType().orElseThrow();
    if (PrimitiveTypes.isPrimitive(type)) {
      // Every primitive arrives as text, so the definitions are what turn it back into a number or
      // a boolean, and what decide whether the text is one.
      return PrimitiveValues.storedValue(source, type, target, path);
    }
    final DefinitionCanonicalStructure child = canonical.elementStructure(entry).orElseThrow();
    // An extension needs no case of its own. This layout stores one inline (FR-003), and inline is
    // what the definitions already describe: an ordinary structure, on the structure carrying it,
    // recursing as far as the source does. The previous layout needed a case because it hoisted
    // every extension into a map at the root of the resource; this one stores it where the
    // definitions put it, so the ordinary structural mapping is the whole of it.
    return structure(child, source, target, observed, path);
  }

  /**
   * Returns the stored value of a complex element, descending through an array where the element
   * repeats. The lambda takes the whole element rather than a leaf of it, which is what keeps a
   * pruned read from reducing to a leaf an individual file may not carry.
   */
  @Nonnull
  private Column structure(
      @Nonnull final DefinitionCanonicalStructure canonical,
      @Nonnull final Column source,
      @Nonnull final DataType target,
      @Nonnull final DataType observed,
      @Nonnull final String path) {
    if (target instanceof final ArrayType array) {
      final DataType element = elementTypeOf(observed);
      return functions.transform(
          source, value -> structure(canonical, value, array.elementType(), element, path));
    }
    final StructType targetStructure = (StructType) target;
    final StructType observedStructure = (StructType) observed;
    final Column[] fields =
        Stream.of(targetStructure.fields())
            .map(
                field ->
                    column(canonical, field, observedStructure, source::getField, path)
                        .alias(field.name()))
            .toArray(Column[]::new);
    // A structure the source did not carry stays absent rather than becoming a structure of nulls,
    // which is what the round trip needs of it.
    return functions.when(source.isNotNull(), functions.struct(fields));
  }

  /**
   * Returns the content a source carries that this layout does not store, as values.
   *
   * <p>Two questions are asked, and they are different questions (decision 59). The definitions are
   * silent about some of it, which is the strictness switch's own subject. The rest the definitions
   * describe, but the bounds configured for the dense mode drop it, and the caller's remedy there
   * is to raise the bound rather than to correct the data. Both are detectable, which is what
   * FR-017 and FR-018 require; returning them rather than only logging them is what makes them so.
   *
   * @param resourceType the type of the resources the source carries
   * @param observed the schema the source was read with
   * @return the findings, in the order they were reached
   */
  @Nonnull
  public List<NonConformantContent> findings(
      @Nonnull final String resourceType, @Nonnull final StructType observed) {
    final DefinitionCanonicalStructure canonical =
        DefinitionCanonicalStructure.forResource(definitions, resourceType);
    final List<NonConformantContent> findings =
        new ArrayList<>(StrictnessCheck.of(canonical, resourceType).check(observed));
    if (configuration.isDenseSchema()) {
      findings.addAll(
          BoundsCheck.of(canonical, schemaBuilder.dense(resourceType), resourceType)
              .check(observed));
    }
    return List.copyOf(findings);
  }

  /** Raises or logs the content this layout does not store, as the strictness switch decides. */
  private void report(@Nonnull final List<NonConformantContent> findings) {
    if (findings.isEmpty()) {
      return;
    }
    final String detail =
        findings.stream().map(NonConformantContent::toString).collect(Collectors.joining("; "));
    if (configuration.isFailOnNonConformantContent()) {
      throw new InvalidUserInputError("Content that this layout does not store: " + detail);
    }
    // Ignoring is the default, and ignoring is not silence (FR-018).
    log.warn("Ignoring content that this layout does not store: {}", detail);
  }

  /**
   * Returns the part of an observed schema that decides what is stored, which is what pruning is
   * asked about.
   *
   * <p>The metadata groups are removed because the transform does not populate them: a schema
   * fitted to the data is the schema of what is written, so a group nothing writes to would
   * otherwise appear as a column that is null in every row. The content those keys carried is
   * reported rather than dropped in silence, which is what FR-017's carve-out requires of it until
   * the group is populated.
   */
  @Nonnull
  private static StructType stored(@Nonnull final StructType observed) {
    final StructField[] fields =
        Stream.of(observed.fields())
            .filter(field -> !field.name().startsWith(LayoutFields.METADATA_GROUP_PREFIX))
            .map(
                field ->
                    new StructField(
                        field.name(),
                        storedType(field.dataType()),
                        field.nullable(),
                        field.metadata()))
            .toArray(StructField[]::new);
    return new StructType(fields);
  }

  @Nonnull
  private static DataType storedType(@Nonnull final DataType type) {
    if (type instanceof final ArrayType array) {
      return DataTypes.createArrayType(storedType(array.elementType()), array.containsNull());
    }
    return type instanceof final StructType structure ? stored(structure) : type;
  }

  /**
   * Returns whether the source carried an element in the shape the definitions declare for it. The
   * target type is the authority on both questions, because it was derived from the definitions:
   * whether the element repeats, and whether it is a structure or a leaf.
   */
  private static boolean shapeMatches(
      @Nonnull final DataType target, @Nonnull final DataType observed) {
    if (target instanceof ArrayType != observed instanceof ArrayType) {
      return false;
    }
    return elementTypeOf(target) instanceof StructType
        == elementTypeOf(observed) instanceof StructType;
  }

  @Nonnull
  private static Optional<StructField> observedField(
      @Nonnull final StructType observed, @Nonnull final String name) {
    return Stream.of(observed.fields()).filter(field -> field.name().equals(name)).findFirst();
  }

  /** Returns the type of the values a column carries, unwrapping the array where it repeats. */
  @Nonnull
  private static DataType elementTypeOf(@Nonnull final DataType type) {
    return type instanceof final ArrayType array ? elementTypeOf(array.elementType()) : type;
  }
}
