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

import au.csiro.pathling.encoders.ExtensionSupport;
import au.csiro.pathling.encoders.QuantitySupport;
import au.csiro.pathling.encoders.datatypes.DecimalCustomCoder;
import au.csiro.pathling.schema.LayoutFields;
import jakarta.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.stream.Stream;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Classifies a resource table by its layout, from its schema alone (US7, FR-037 to FR-039).
 *
 * <p>Classification is by structural markers rather than by a full comparison with an expected
 * schema, because a fitted schema carries only the elements its data populates and so has no single
 * expected shape. A marker is a field that only one layout ever writes, recognised by its name and
 * type together:
 *
 * <ul>
 *   <li>The previous layout's markers are the {@code _fid} beside every complex element, the {@code
 *       _extension} map at the root, the {@code _scale} companion beside a decimal, the {@code
 *       id_versioned} companion beside the id, and the {@code _value_canonicalized} and {@code
 *       _code_canonicalized} fields of a quantity. With extensions disabled the previous encoder
 *       writes no {@code _fid} or {@code _extension}, but the versioned id is still present.
 *   <li>The Parquet on FHIR layout's markers are {@code resourceType} at the root, a primitive's
 *       metadata group and an annotation. Only the first is written before the metadata groups and
 *       annotations are populated, so a sparse new-layout table is recognised by it alone.
 * </ul>
 *
 * <p>A leading underscore is not a marker on its own, since the metadata groups and annotations of
 * the new layout begin with one too.
 *
 * <p>The decisive marker of the previous layout is the {@code id_versioned} beside a string {@code
 * id} at the root, which the previous encoder writes for every resource, with extensions enabled or
 * not (decision 100). Where it is present the table is the previous layout, whatever else it
 * carries: a table holding both layouts, as an append of new data into an earlier table produces,
 * keeps it, and is the case the engine assumes never arises (decision 47), so it is refused rather
 * than read. The other previous-layout markers are evidence, cited in a refusal, but do not
 * classify a table by themselves. The root versioned id is present in every previous-layout table,
 * whether or not the previous encoder stored extensions, so the other markers would add no table it
 * misses, and each of them would add a way to misclassify a conforming table that happens to carry
 * a field of that name.
 *
 * <p>Without the root versioned id, a schema is the Parquet on FHIR layout where it carries a
 * marker of that layout, and is otherwise unclassifiable and accepted, because marker absence is
 * not evidence of an unsupported layout.
 *
 * <p>Detection reads nothing but the schema it is given, so it needs no file access beyond the
 * schema the read already obtains (FR-038).
 *
 * @author Piotr Szul
 */
public final class LayoutDetector {

  /** The largest number of markers a refusal cites before abbreviating the rest. */
  public static final int MAX_CITED_MARKERS = 5;

  @Nonnull private static final String VERSIONED_ID = "id_versioned";

  @Nonnull private static final String ID = "id";

  @Nonnull
  private static final Set<String> QUANTITY_CANONICAL_FIELDS =
      Set.of(
          QuantitySupport.VALUE_CANONICALIZED_FIELD_NAME(),
          QuantitySupport.CODE_CANONICALIZED_FIELD_NAME());

  @Nonnull private static final String PATH_SEPARATOR = ".";

  private LayoutDetector() {}

  /**
   * Classifies a schema by its layout.
   *
   * @param schema the schema of a resource table
   * @return the layout, and the markers of each layout that were found
   */
  @Nonnull
  public static LayoutDetection detect(@Nonnull final StructType schema) {
    final List<String> previous = markers(schema, "", LayoutDetector::isPreviousLayoutMarker);
    final List<String> current = markers(schema, "", LayoutDetector::isNewLayoutMarker);
    final StorageLayout layout;
    if (hasRootVersionedId(schema)) {
      layout = StorageLayout.PREVIOUS;
    } else if (!current.isEmpty()) {
      layout = StorageLayout.PARQUET_ON_FHIR;
    } else {
      layout = StorageLayout.UNCLASSIFIABLE;
    }
    return new LayoutDetection(layout, previous, current);
  }

  /**
   * Classifies a schema and refuses it if its layout is not accepted.
   *
   * @param resourceType the resource type the table holds, for the message
   * @param schema the schema of the table
   * @return the classification, where the layout is accepted
   * @throws UnsupportedLayoutException if the schema is in a layout this release does not read
   */
  @Nonnull
  public static LayoutDetection check(
      @Nonnull final String resourceType, @Nonnull final StructType schema) {
    final LayoutDetection detection = detect(schema);
    if (!detection.isSupported()) {
      throw new UnsupportedLayoutException(
          message(resourceType, detection), resourceType, detection);
    }
    return detection;
  }

  /**
   * Builds the message refusing a table, naming the resource type, the detected layout, the
   * expected layout, the evidence and the remedy. The remedy is a re-import from source, into a new
   * location or by overwriting the existing one, because no in-place conversion exists and any
   * rewrite of the previous layout's fixed-precision decimals would be lossy where a re-import is
   * not. An overwrite is not refused, since it leaves one layout behind (decision 105).
   *
   * <p>Where the data mixes the two layouts, a re-import cannot append to or merge into the data,
   * which is refused too (decision 99), so it goes to a new location or overwrites. The message
   * also covers a directory of files, whose files of the earlier layout can be removed instead, so
   * that it serves every source and sink without being told where the data lives.
   *
   * @param resourceType the resource type the table holds
   * @param detection the classification of its schema
   * @return the message
   */
  @Nonnull
  static String message(
      @Nonnull final String resourceType, @Nonnull final LayoutDetection detection) {
    final String expected = StorageLayout.PARQUET_ON_FHIR.getDescription();
    final StringBuilder message =
        new StringBuilder()
            .append("The ")
            .append(resourceType)
            .append(" data is stored in ")
            .append(detection.getLayout().getDescription())
            .append(", not ")
            .append(expected)
            .append(" this release expects. Fields identifying the earlier layout: ")
            .append(cite(detection.getPreviousLayoutMarkers()))
            .append(". ");
    if (detection.isMixed()) {
      return message
          .append("It also carries fields of the Parquet on FHIR layout (")
          .append(cite(detection.getNewLayoutMarkers()))
          .append("), so it mixes the two layouts, as appending new data to earlier data does. ")
          .append("Re-import the data from its source with this release, into a new location or ")
          .append("by overwriting this one, because appending to or merging into this data is ")
          .append("refused, and data in the ")
          .append("earlier layout cannot be converted in place. Where the data is a directory of ")
          .append("files, the directory holds files of the earlier layout: remove them, or ")
          .append("re-import their data from its source.")
          .toString();
    }
    return message
        .append("Re-import the data from its source with this release, into a new location or ")
        .append("by overwriting this one; ")
        .append("data in the earlier layout cannot be converted in place.")
        .toString();
  }

  @Nonnull
  private static String cite(@Nonnull final List<String> markers) {
    final String cited = String.join(", ", markers.stream().limit(MAX_CITED_MARKERS).toList());
    return markers.size() > MAX_CITED_MARKERS
        ? cited + " and " + (markers.size() - MAX_CITED_MARKERS) + " more"
        : cited;
  }

  /**
   * Finds the fields of a structure, and of every structure nested within it, that satisfy a marker
   * test, in schema order and depth first.
   */
  @Nonnull
  private static List<String> markers(
      @Nonnull final StructType struct,
      @Nonnull final String prefix,
      @Nonnull final BiPredicate<StructField, StructType> isMarker) {
    final boolean root = prefix.isEmpty();
    return Arrays.stream(struct.fields())
        .flatMap(
            field -> {
              final String path = prefix + field.name();
              final Stream<String> own =
                  isMarker.test(field, struct) && (root || !isRootOnly(field))
                      ? Stream.of(path)
                      : Stream.empty();
              return Stream.concat(
                  own, nested(field.dataType(), path + PATH_SEPARATOR, isMarker).stream());
            })
        .toList();
  }

  /** Descends through arrays and map values to the structures they hold. */
  @Nonnull
  private static List<String> nested(
      @Nonnull final DataType type,
      @Nonnull final String prefix,
      @Nonnull final BiPredicate<StructField, StructType> isMarker) {
    return switch (type) {
      case final StructType struct -> markers(struct, prefix, isMarker);
      case final ArrayType array -> nested(array.elementType(), prefix, isMarker);
      case final MapType map -> nested(map.valueType(), prefix, isMarker);
      default -> List.of();
    };
  }

  /** The markers that are only markers at the root of a table. */
  private static boolean isRootOnly(@Nonnull final StructField field) {
    return ExtensionSupport.EXTENSIONS_FIELD_NAME().equals(field.name())
        || LayoutFields.RESOURCE_TYPE.equals(field.name());
  }

  private static boolean isPreviousLayoutMarker(
      @Nonnull final StructField field, @Nonnull final StructType parent) {
    final String name = field.name();
    final DataType type = field.dataType();
    final boolean fid =
        ExtensionSupport.FID_FIELD_NAME().equals(name) && DataTypes.IntegerType.equals(type);
    final boolean extensionTable =
        ExtensionSupport.EXTENSIONS_FIELD_NAME().equals(name) && type instanceof MapType;
    return fid
        || extensionTable
        || QUANTITY_CANONICAL_FIELDS.contains(name)
        || isVersionedId(field, parent)
        || isScaleCompanion(field, parent);
  }

  /** Whether the root of a schema carries the versioned id, the decisive previous-layout marker. */
  private static boolean hasRootVersionedId(@Nonnull final StructType schema) {
    return Arrays.stream(schema.fields()).anyMatch(field -> isVersionedId(field, schema));
  }

  /** The versioned id is a string beside a string id. */
  private static boolean isVersionedId(
      @Nonnull final StructField field, @Nonnull final StructType parent) {
    return VERSIONED_ID.equals(field.name())
        && DataTypes.StringType.equals(field.dataType())
        && sibling(parent, ID)
            .map(StructField::dataType)
            .filter(DataTypes.StringType::equals)
            .isPresent();
  }

  /** A scale companion holds integers and sits beside a field of the same stem holding decimals. */
  private static boolean isScaleCompanion(
      @Nonnull final StructField field, @Nonnull final StructType parent) {
    final String name = field.name();
    final String suffix = DecimalCustomCoder.SCALE_SUFFIX();
    return name.endsWith(suffix)
        && name.length() > suffix.length()
        && holds(field.dataType(), DataTypes.IntegerType)
        && sibling(parent, name.substring(0, name.length() - suffix.length()))
            .map(StructField::dataType)
            .filter(LayoutDetector::holdsDecimals)
            .isPresent();
  }

  private static boolean isNewLayoutMarker(
      @Nonnull final StructField field, @Nonnull final StructType parent) {
    final String name = field.name();
    final boolean resourceType =
        LayoutFields.RESOURCE_TYPE.equals(name) && DataTypes.StringType.equals(field.dataType());
    return resourceType
        || name.startsWith(LayoutFields.ANNOTATION_PREFIX)
        || isMetadataGroup(field, parent);
  }

  /**
   * A metadata group is a structure, or an array of them, carrying nothing but the group's fields,
   * named for the primitive it accompanies, which is present beside it.
   */
  private static boolean isMetadataGroup(
      @Nonnull final StructField field, @Nonnull final StructType parent) {
    final String name = field.name();
    final String prefix = LayoutFields.METADATA_GROUP_PREFIX;
    return name.startsWith(prefix)
        && !name.startsWith(LayoutFields.ANNOTATION_PREFIX)
        && sibling(parent, name.substring(prefix.length())).isPresent()
        && groupStruct(field.dataType())
            .filter(struct -> struct.fields().length > 0)
            .filter(
                struct ->
                    Arrays.stream(struct.fieldNames())
                        .allMatch(LayoutFields.METADATA_GROUP_FIELDS::contains))
            .isPresent();
  }

  @Nonnull
  private static Optional<StructType> groupStruct(@Nonnull final DataType type) {
    return switch (type) {
      case final StructType struct -> Optional.of(struct);
      case final ArrayType array when array.elementType() instanceof final StructType struct ->
          Optional.of(struct);
      default -> Optional.empty();
    };
  }

  @Nonnull
  private static Optional<StructField> sibling(
      @Nonnull final StructType parent, @Nonnull final String name) {
    return Arrays.stream(parent.fields())
        .filter(candidate -> candidate.name().equals(name))
        .findFirst();
  }

  private static boolean holds(@Nonnull final DataType type, @Nonnull final DataType leaf) {
    return type instanceof final ArrayType array
        ? holds(array.elementType(), leaf)
        : leaf.equals(type);
  }

  private static boolean holdsDecimals(@Nonnull final DataType type) {
    return type instanceof final ArrayType array
        ? holdsDecimals(array.elementType())
        : type instanceof DecimalType;
  }
}
