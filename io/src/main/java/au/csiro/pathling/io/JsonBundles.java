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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Splits a FHIR JSON bundle into the resources of one type it carries, each as the raw JSON text of
 * its entry's {@code resource}, beside what each URN it mentions resolves to among the bundle's
 * entries (ticket 32, decision 83).
 *
 * <p>The bundle is read once, with a streaming parser, and nothing is built from it but the text of
 * each resource wanted and what resolution needs of every entry: its full URL and its resource's
 * type, id and version. Each resource is then read as any other JSON document of its type, so the
 * bundle is never inferred as a whole, and a field that two resource types declare differently is
 * not widened by the other. Which of the URNs a resource mentions are references is for the
 * definitions to say, so every string held by a field named {@code reference} that begins with a
 * URN prefix is given its resolution, where the bundle has one.
 *
 * <p>Text that is not JSON fails, as does a document that is not a bundle, naming it. What the JSON
 * route accepts silently in a single document is accepted here too: text after the bundle is
 * ignored, and of two values for one key the last is kept (decision 134).
 */
final class JsonBundles {

  @Nonnull private static final JsonFactory FACTORY = new JsonFactory();

  @Nonnull private static final String RESOURCE_TYPE = "resourceType";

  private JsonBundles() {}

  /**
   * Returns the resources of one type that a bundle carries.
   *
   * @param bundle the bundle
   * @param resourceType the type of the resources wanted
   * @return the resources and the resolutions of the URNs each mentions, in the bundle's order
   * @throws IllegalArgumentException if the text is not JSON, or not a bundle
   */
  @Nonnull
  static List<BundleResource> split(
      @Nullable final String bundle, @Nonnull final String resourceType) {
    if (bundle == null) {
      return List.of();
    }
    final List<BundleEntryScan> entries = new ArrayList<>();
    final String type;
    try (final JsonParser parser = FACTORY.createParser(bundle)) {
      type = scan(parser, bundle, resourceType, entries);
    } catch (final IOException | IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Cannot read the FHIR JSON bundle "
              + DocumentExcerpt.startOf(bundle)
              + ": "
              + e.getMessage());
    }
    if (!StorableResourceType.BUNDLE.equals(type)) {
      throw new IllegalArgumentException(
          "Expected a bundle and found "
              + (type == null ? "a document with no resource type" : "a resource of type " + type)
              + ": "
              + DocumentExcerpt.startOf(bundle));
    }
    return BundleEntryScan.resolve(entries);
  }

  /**
   * Reads a bundle, which the parser is before, adding each of its entries to those given, and
   * returns its resource type, or null where it has none.
   */
  @Nullable
  private static String scan(
      @Nonnull final JsonParser parser,
      @Nonnull final String bundle,
      @Nonnull final String resourceType,
      @Nonnull final List<BundleEntryScan> entries)
      throws IOException {
    if (parser.nextToken() != JsonToken.START_OBJECT) {
      throw new IllegalArgumentException("A bundle is not a JSON object");
    }
    String type = null;
    while (parser.nextToken() == JsonToken.FIELD_NAME) {
      final String field = parser.currentName();
      final JsonToken value = parser.nextToken();
      if (RESOURCE_TYPE.equals(field) && value == JsonToken.VALUE_STRING) {
        type = parser.getText();
      } else if ("entry".equals(field) && value == JsonToken.START_ARRAY) {
        while (parser.nextToken() != JsonToken.END_ARRAY) {
          entries.add(entry(parser, bundle, resourceType));
        }
      } else {
        parser.skipChildren();
      }
    }
    return type;
  }

  /** Reads one entry, which the parser is on the start of, leaving it on the entry's end. */
  @Nonnull
  private static BundleEntryScan entry(
      @Nonnull final JsonParser parser,
      @Nonnull final String bundle,
      @Nonnull final String resourceType)
      throws IOException {
    final BundleEntryScan entry = new BundleEntryScan();
    if (parser.currentToken() != JsonToken.START_OBJECT) {
      parser.skipChildren();
      return entry;
    }
    while (parser.nextToken() == JsonToken.FIELD_NAME) {
      final String field = parser.currentName();
      final JsonToken value = parser.nextToken();
      if ("fullUrl".equals(field) && value == JsonToken.VALUE_STRING) {
        entry.setFullUrl(parser.getText());
      } else if ("resource".equals(field) && value == JsonToken.START_OBJECT) {
        resource(parser, bundle, resourceType, entry);
      } else {
        parser.skipChildren();
      }
    }
    return entry;
  }

  /**
   * Reads an entry's resource, which the parser is on the start of, leaving it on the resource's
   * end. Its text is taken from the bundle by offset, so it is the source's text exactly.
   */
  private static void resource(
      @Nonnull final JsonParser parser,
      @Nonnull final String bundle,
      @Nonnull final String resourceType,
      @Nonnull final BundleEntryScan entry)
      throws IOException {
    final int start = (int) parser.currentTokenLocation().getCharOffset();
    final Set<String> urns = new LinkedHashSet<>();
    String type = null;
    String id = null;
    String versionId = null;
    while (parser.nextToken() == JsonToken.FIELD_NAME) {
      final String field = parser.currentName();
      final JsonToken value = parser.nextToken();
      if (value == JsonToken.VALUE_STRING && RESOURCE_TYPE.equals(field)) {
        type = parser.getText();
      } else if (value == JsonToken.VALUE_STRING && "id".equals(field)) {
        id = parser.getText();
      } else if (value == JsonToken.START_OBJECT && "meta".equals(field)) {
        versionId = meta(parser, urns, versionId);
      } else {
        mentions(parser, field, urns);
      }
    }
    final int end = (int) parser.currentTokenLocation().getCharOffset() + 1;
    if (type != null) {
      entry.setResource(
          type,
          id,
          versionId,
          resourceType.equals(type) ? bundle.substring(start, end) : null,
          urns);
    }
  }

  /**
   * Reads a resource's meta, which the parser is on the start of, leaving it on the meta's end, and
   * returns the version it names, or the version given where it names none.
   */
  @Nullable
  private static String meta(
      @Nonnull final JsonParser parser,
      @Nonnull final Set<String> urns,
      @Nullable final String versionId)
      throws IOException {
    String version = versionId;
    while (parser.nextToken() == JsonToken.FIELD_NAME) {
      final String field = parser.currentName();
      if (parser.nextToken() == JsonToken.VALUE_STRING && "versionId".equals(field)) {
        version = parser.getText();
      } else {
        mentions(parser, field, urns);
      }
    }
    return version;
  }

  /**
   * Reads the value of a field, which the parser is on, leaving it on the value's end, and adds to
   * the URNs mentioned each string within it that a field named {@code reference} holds and that
   * begins with a URN prefix. A string in an array is taken to be held by the field named last.
   */
  private static void mentions(
      @Nonnull final JsonParser parser,
      @Nonnull final String field,
      @Nonnull final Set<String> urns)
      throws IOException {
    String name = field;
    int depth = 0;
    JsonToken token = parser.currentToken();
    while (true) {
      switch (token) {
        case START_OBJECT, START_ARRAY -> depth++;
        case END_OBJECT, END_ARRAY -> depth--;
        case FIELD_NAME -> name = parser.currentName();
        // Only the text resolution needs is read, so a value of any other field, however long, is
        // skipped without being held.
        case VALUE_STRING -> {
          if ("reference".equals(name)) {
            final String reference = parser.getText();
            if (reference.startsWith(BundleResource.URN)) {
              urns.add(reference);
            }
          }
        }
        default -> {
          // Other values say nothing resolution needs.
        }
      }
      if (depth == 0) {
        return;
      }
      token = parser.nextToken();
    }
  }
}
