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

  /** How many characters of a document name it, where its id is not known. */
  private static final int EXCERPT_LENGTH = 80;

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
    String type = null;
    try (final JsonParser parser = FACTORY.createParser(bundle)) {
      if (parser.nextToken() != JsonToken.START_OBJECT) {
        throw new IllegalArgumentException("A bundle is not a JSON object");
      }
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
    } catch (final IOException | IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Cannot read the FHIR JSON bundle " + name(bundle) + ": " + e.getMessage());
    }
    if (!StorableResourceType.BUNDLE.equals(type)) {
      throw new IllegalArgumentException(
          "Expected a bundle and found "
              + (type == null ? "a document with no resource type" : "a resource of type " + type)
              + ": "
              + name(bundle));
    }
    return BundleEntryScan.resolve(entries);
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
    final Set<String> urns = XmlDocuments.orderedSet();
    String type = null;
    String id = null;
    String versionId = null;
    String field = null;
    int depth = 0;
    boolean inMeta = false;
    JsonToken token = parser.currentToken();
    while (true) {
      switch (token) {
        case START_OBJECT, START_ARRAY -> {
          depth++;
          if (depth == 2) {
            inMeta = "meta".equals(field) && token == JsonToken.START_OBJECT;
          }
        }
        case END_OBJECT, END_ARRAY -> {
          depth--;
          if (depth == 1) {
            inMeta = false;
          }
        }
        case FIELD_NAME -> field = parser.currentName();
        case VALUE_STRING -> {
          final String text = parser.getText();
          if (depth == 1 && RESOURCE_TYPE.equals(field)) {
            type = text;
          } else if (depth == 1 && "id".equals(field)) {
            id = text;
          } else if (depth == 2 && inMeta && "versionId".equals(field)) {
            versionId = text;
          }
          if ("reference".equals(field) && text.startsWith(XmlDocuments.URN)) {
            urns.add(text);
          }
        }
        default -> {
          // Other values say nothing resolution needs.
        }
      }
      if (depth == 0) {
        break;
      }
      token = parser.nextToken();
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

  /** Names a bundle by its first characters. */
  @Nonnull
  private static String name(@Nonnull final String bundle) {
    final String start = bundle.strip();
    return "starting \""
        + (start.length() > EXCERPT_LENGTH ? start.substring(0, EXCERPT_LENGTH) + "…" : start)
        + "\"";
  }
}
