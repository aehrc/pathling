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

package au.csiro.pathling.terminology.store;

import au.csiro.pathling.terminology.conceptmap.ConceptMapFault;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.exceptions.FHIRException;
import org.hl7.fhir.r4.model.codesystems.ConceptMapEquivalence;

/**
 * Walks the Jackson token stream of a single FHIR R4 ConceptMap, writing one staging row for each
 * target of each source element. Each row records the index of its group rather than the group's
 * systems, which may follow the elements in the source, so the flattener never holds more than one
 * element at a time and accepts the fields of the map in any order.
 *
 * <p>The parts of a ConceptMap kept are those that local {@code translate} uses, the group source
 * and target systems, the element code, and each target's code and equivalence, together with those
 * that reading the map as a concept map relation adds: the group source and target versions and the
 * element and target displays. For {@code translate}, a target without an equivalence is {@code
 * relatedto}, and an element without a code is skipped, since it cannot be looked up.
 *
 * <p>A concept map relation cannot carry all of that content, so the first fault in the map is
 * recorded with the staging, in the order {@link
 * au.csiro.pathling.terminology.conceptmap.ConceptMapContent#fromResource} checks a resource: each
 * group in turn, its {@code source} before its elements, each element's code before its targets,
 * and each target's {@code dependsOn}, {@code product}, equivalence and code in that order. A value
 * that is absent, blank, or carried only by an extension is treated as missing.
 *
 * @author John Grimes
 */
@Slf4j
public class ConceptMapStreamFlattener {

  /** The number of mappings between running-count progress messages. */
  private static final long PROGRESS_INTERVAL = 1_000_000L;

  private static final String FIELD_GROUP = "group";
  private static final String FIELD_SOURCE = "source";
  private static final String FIELD_SOURCE_VERSION = "sourceVersion";
  private static final String FIELD_TARGET = "target";
  private static final String FIELD_TARGET_VERSION = "targetVersion";
  private static final String FIELD_ELEMENT = "element";
  private static final String FIELD_CODE = "code";
  private static final String FIELD_DISPLAY = "display";
  private static final String FIELD_EQUIVALENCE = "equivalence";
  private static final String FIELD_DEPENDS_ON = "dependsOn";
  private static final String FIELD_PRODUCT = "product";

  @Nonnull private final ConceptMapStaging staging;

  /**
   * Creates a flattener that writes into the given staging.
   *
   * @param staging the staging to append rows to
   */
  public ConceptMapStreamFlattener(@Nonnull final ConceptMapStaging staging) {
    this.staging = staging;
  }

  /**
   * Flattens a ConceptMap from the current parser position, appending a staging row per mapping,
   * registering the systems and versions of each group with the staging, and recording the first
   * fault in the map, if any.
   *
   * @param parser a parser positioned before the ConceptMap object
   * @return the number of mappings flattened
   * @throws IOException if the stream cannot be read
   * @throws TerminologyImportException if the source is not a JSON object or carries an
   *     unrecognised equivalence
   */
  public int flatten(@Nonnull final JsonParser parser) throws IOException {
    if (parser.nextToken() != JsonToken.START_OBJECT) {
      throw new TerminologyImportException("Expected a ConceptMap JSON object");
    }
    while (parser.nextToken() == JsonToken.FIELD_NAME) {
      final String field = parser.currentName();
      parser.nextToken();
      if (FIELD_GROUP.equals(field) && parser.currentToken() == JsonToken.START_ARRAY) {
        while (parser.nextToken() != JsonToken.END_ARRAY) {
          flattenGroup(parser);
        }
      } else {
        parser.skipChildren();
      }
    }
    return staging.mappingCount();
  }

  private void flattenGroup(@Nonnull final JsonParser parser) throws IOException {
    if (parser.currentToken() != JsonToken.START_OBJECT) {
      parser.skipChildren();
      return;
    }
    final int group = staging.groupCount();
    String sourceSystem = null;
    String sourceVersion = null;
    String targetSystem = null;
    String targetVersion = null;
    String elementFault = null;
    while (parser.nextToken() == JsonToken.FIELD_NAME) {
      final String field = parser.currentName();
      parser.nextToken();
      switch (field) {
        case FIELD_SOURCE -> sourceSystem = text(parser);
        case FIELD_SOURCE_VERSION -> sourceVersion = text(parser);
        case FIELD_TARGET -> targetSystem = text(parser);
        case FIELD_TARGET_VERSION -> targetVersion = text(parser);
        case FIELD_ELEMENT -> {
          final String fault = flattenElements(parser, group);
          elementFault = elementFault == null ? fault : elementFault;
        }
        default -> parser.skipChildren();
      }
    }
    staging.appendGroup(sourceSystem, sourceVersion, targetSystem, targetVersion);
    // The source of a group is checked before any of its elements, wherever it appears.
    if (!present(sourceSystem)) {
      staging.recordFault(ConceptMapFault.groupWithoutSource());
    } else if (elementFault != null) {
      staging.recordFault(elementFault);
    }
  }

  /** Flattens the elements of a group, returning the first fault among them, or null. */
  @Nullable
  private String flattenElements(@Nonnull final JsonParser parser, final int group)
      throws IOException {
    if (parser.currentToken() != JsonToken.START_ARRAY) {
      parser.skipChildren();
      return null;
    }
    String fault = null;
    while (parser.nextToken() != JsonToken.END_ARRAY) {
      final String elementFault = flattenElement(parser, group);
      fault = fault == null ? elementFault : fault;
    }
    return fault;
  }

  /**
   * Flattens one element, returning its fault, or null. Its targets may precede its code, so they
   * are held until the element ends, which bounds the memory by the size of the element rather than
   * of the map.
   */
  @Nullable
  private String flattenElement(@Nonnull final JsonParser parser, final int group)
      throws IOException {
    if (parser.currentToken() != JsonToken.START_OBJECT) {
      parser.skipChildren();
      return null;
    }
    String code = null;
    String display = null;
    final List<Target> targets = new ArrayList<>();
    while (parser.nextToken() == JsonToken.FIELD_NAME) {
      final String field = parser.currentName();
      parser.nextToken();
      switch (field) {
        case FIELD_CODE -> code = text(parser);
        case FIELD_DISPLAY -> display = text(parser);
        case FIELD_TARGET -> readTargets(parser, targets);
        default -> parser.skipChildren();
      }
    }
    if (code != null) {
      for (final Target target : targets) {
        staging.appendMapping(
            group, code, display, target.code(), target.display(), target.equivalence());
        if (staging.mappingCount() % PROGRESS_INTERVAL == 0) {
          log.info("Parsed {} mappings", staging.mappingCount());
        }
      }
    }
    return fault(code, targets);
  }

  /** Returns the first fault of an element, checking its code and then each target in turn. */
  @Nullable
  private static String fault(@Nullable final String code, @Nonnull final List<Target> targets) {
    if (!present(code)) {
      return ConceptMapFault.elementWithoutCode();
    }
    for (final Target target : targets) {
      if (target.dependsOn()) {
        return ConceptMapFault.dependsOn(code);
      }
      if (target.product()) {
        return ConceptMapFault.product(code);
      }
      if (!target.equivalenceGiven()) {
        return ConceptMapFault.noEquivalence(code);
      }
      if (!ConceptMapEquivalence.UNMATCHED.toCode().equals(target.equivalence())
          && !present(target.code())) {
        return ConceptMapFault.noTargetCode(code);
      }
    }
    return null;
  }

  /** Reads an element's targets. */
  private static void readTargets(
      @Nonnull final JsonParser parser, @Nonnull final List<Target> targets) throws IOException {
    if (parser.currentToken() != JsonToken.START_ARRAY) {
      parser.skipChildren();
      return;
    }
    while (parser.nextToken() != JsonToken.END_ARRAY) {
      if (parser.currentToken() != JsonToken.START_OBJECT) {
        parser.skipChildren();
        continue;
      }
      String code = null;
      String display = null;
      String equivalence = null;
      boolean dependsOn = false;
      boolean product = false;
      while (parser.nextToken() == JsonToken.FIELD_NAME) {
        final String field = parser.currentName();
        parser.nextToken();
        switch (field) {
          case FIELD_CODE -> code = text(parser);
          case FIELD_DISPLAY -> display = text(parser);
          case FIELD_EQUIVALENCE -> equivalence = text(parser);
          case FIELD_DEPENDS_ON -> dependsOn = hasContent(parser);
          case FIELD_PRODUCT -> product = hasContent(parser);
          default -> parser.skipChildren();
        }
      }
      targets.add(
          new Target(
              code,
              display,
              validEquivalence(equivalence),
              equivalence != null && !equivalence.isEmpty(),
              dependsOn,
              product));
    }
  }

  /** Returns the value of a primitive, or null where it is not one, consuming it either way. */
  @Nullable
  private static String text(@Nonnull final JsonParser parser) throws IOException {
    if (parser.currentToken().isScalarValue()) {
      return parser.getValueAsString();
    }
    parser.skipChildren();
    return null;
  }

  /**
   * Reports whether a repeating element has an entry with content, as HAPI's {@code has} methods do
   * for {@code dependsOn} and {@code product}, consuming it.
   */
  private static boolean hasContent(@Nonnull final JsonParser parser) throws IOException {
    if (parser.currentToken() != JsonToken.START_ARRAY) {
      parser.skipChildren();
      return false;
    }
    boolean content = false;
    while (parser.nextToken() != JsonToken.END_ARRAY) {
      if (parser.currentToken() != JsonToken.START_OBJECT) {
        parser.skipChildren();
        continue;
      }
      while (parser.nextToken() == JsonToken.FIELD_NAME) {
        content = true;
        parser.nextToken();
        parser.skipChildren();
      }
    }
    return content;
  }

  /** Reports whether a value is present, as HAPI reports a primitive with a value. */
  private static boolean present(@Nullable final String value) {
    return value != null && !value.isBlank();
  }

  /** Returns the equivalence code, defaulting to {@code relatedto} and rejecting unknown codes. */
  @Nonnull
  private static String validEquivalence(@Nullable final String code) {
    if (code == null || code.isEmpty()) {
      return ConceptMapEquivalence.RELATEDTO.toCode();
    }
    try {
      return ConceptMapEquivalence.fromCode(code).toCode();
    } catch (final FHIRException e) {
      throw new TerminologyImportException("Unrecognised ConceptMap equivalence '" + code + "'", e);
    }
  }

  /**
   * One target of an element, as the map gives it.
   *
   * @param code the target code, or null
   * @param display the target display, or null
   * @param equivalence the equivalence staged for {@code translate}
   * @param equivalenceGiven whether the map gives the equivalence rather than it being defaulted
   * @param dependsOn whether the target depends on other elements
   * @param product whether the target carries products
   */
  private record Target(
      @Nullable String code,
      @Nullable String display,
      @Nonnull String equivalence,
      boolean equivalenceGiven,
      boolean dependsOn,
      boolean product) {}
}
