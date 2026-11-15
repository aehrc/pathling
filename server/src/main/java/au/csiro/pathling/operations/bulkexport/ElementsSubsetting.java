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

package au.csiro.pathling.operations.bulkexport;

import au.csiro.pathling.library.PathlingContext;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import jakarta.annotation.Nonnull;
import java.util.Set;
import java.util.stream.StreamSupport;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

/**
 * Restricts exported resources to the elements requested through the {@code _elements} parameter,
 * marking each with the {@code SUBSETTED} tag.
 *
 * <p>Resources are subset as FHIR JSON rather than as columns, because a stored dataset carries
 * only the elements its data populates, alongside fields the storage layout adds for some of them.
 * Working on the JSON keeps an element together with the id and extensions of its primitive value,
 * and lets the library fit the schema of the result to what remains.
 *
 * @author John Grimes
 */
final class ElementsSubsetting {

  /** The system of the tag marking a subsetted resource. */
  static final String SUBSETTED_SYSTEM =
      "http://terminology.hl7.org/CodeSystem/v3-ObservationValue";

  /** The code of the tag marking a subsetted resource. */
  static final String SUBSETTED_CODE = "SUBSETTED";

  private static final String SUBSETTED_DISPLAY = "Resource encoded in summary mode";

  private static final String FHIR_JSON = "application/fhir+json";

  private static final String RESOURCE_TYPE = "resourceType";

  private static final String PRIMITIVE_METADATA_PREFIX = "_";

  private static final String META = "meta";

  private static final String TAG = "tag";

  private static final String SYSTEM = "system";

  private static final String CODE = "code";

  private static final String DISPLAY = "display";

  private ElementsSubsetting() {}

  /**
   * Restricts resources of one type to the given elements and tags them as subsetted.
   *
   * @param context the Pathling context
   * @param resourceType the type of the resources
   * @param resources the resources
   * @param elements the names of the top-level elements to keep
   * @return the subsetted resources
   */
  @Nonnull
  static Dataset<Row> subset(
      @Nonnull final PathlingContext context,
      @Nonnull final String resourceType,
      @Nonnull final Dataset<Row> resources,
      @Nonnull final Set<String> elements) {
    final Set<String> kept = Set.copyOf(elements);
    final Dataset<String> subsetted =
        context
            .decode(resources, resourceType, FHIR_JSON)
            .map(
                (MapFunction<String, String>) document -> subset(document, kept),
                Encoders.STRING());
    return context.encode(subsetted.toDF(), resourceType);
  }

  /**
   * Restricts a resource in FHIR JSON to the given elements and tags it as subsetted.
   *
   * @param document the resource, as FHIR JSON
   * @param elements the names of the top-level elements to keep
   * @return the subsetted resource, as FHIR JSON
   */
  @Nonnull
  static String subset(@Nonnull final String document, @Nonnull final Set<String> elements) {
    final JsonObject resource = JsonParser.parseString(document).getAsJsonObject();
    final JsonObject result = new JsonObject();
    resource.entrySet().stream()
        .filter(member -> isKept(member.getKey(), elements))
        .forEach(member -> result.add(member.getKey(), member.getValue()));
    addSubsettedTag(result);
    return result.toString();
  }

  /**
   * Returns whether a member of a resource is kept: the resource type, a requested element, or the
   * id and extensions of a requested primitive element.
   */
  private static boolean isKept(@Nonnull final String key, @Nonnull final Set<String> elements) {
    return key.equals(RESOURCE_TYPE)
        || elements.contains(key)
        || key.startsWith(PRIMITIVE_METADATA_PREFIX)
            && elements.contains(key.substring(PRIMITIVE_METADATA_PREFIX.length()));
  }

  /** Adds the SUBSETTED tag to a resource, unless it already carries it. */
  private static void addSubsettedTag(@Nonnull final JsonObject resource) {
    final JsonObject meta = resource.has(META) ? resource.getAsJsonObject(META) : new JsonObject();
    final JsonArray tags = meta.has(TAG) ? meta.getAsJsonArray(TAG) : new JsonArray();
    final boolean tagged =
        StreamSupport.stream(tags.spliterator(), false)
            .filter(JsonElement::isJsonObject)
            .map(JsonElement::getAsJsonObject)
            .anyMatch(
                tag ->
                    hasValue(tag, SYSTEM, SUBSETTED_SYSTEM) && hasValue(tag, CODE, SUBSETTED_CODE));
    if (!tagged) {
      final JsonObject tag = new JsonObject();
      tag.addProperty(SYSTEM, SUBSETTED_SYSTEM);
      tag.addProperty(CODE, SUBSETTED_CODE);
      tag.addProperty(DISPLAY, SUBSETTED_DISPLAY);
      tags.add(tag);
    }
    meta.add(TAG, tags);
    resource.add(META, meta);
  }

  private static boolean hasValue(
      @Nonnull final JsonObject object, @Nonnull final String key, @Nonnull final String value) {
    return object.has(key)
        && object.get(key).isJsonPrimitive()
        && value.equals(object.get(key).getAsString());
  }
}
