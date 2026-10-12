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
 * <p>Only the parts of a ConceptMap that local {@code translate} uses are kept: the group source
 * and target systems, the element code, and each target's code and equivalence. A target without an
 * equivalence is {@code relatedto}, and an element without a code is skipped, since it cannot be
 * looked up.
 *
 * @author John Grimes
 */
@Slf4j
public class ConceptMapStreamFlattener {

  /** The number of mappings between running-count progress messages. */
  private static final long PROGRESS_INTERVAL = 1_000_000L;

  private static final String FIELD_GROUP = "group";
  private static final String FIELD_SOURCE = "source";
  private static final String FIELD_TARGET = "target";
  private static final String FIELD_ELEMENT = "element";
  private static final String FIELD_CODE = "code";
  private static final String FIELD_EQUIVALENCE = "equivalence";

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
   * Flattens a ConceptMap from the current parser position, appending a staging row per mapping and
   * registering the systems of each group with the staging.
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
    String targetSystem = null;
    while (parser.nextToken() == JsonToken.FIELD_NAME) {
      final String field = parser.currentName();
      parser.nextToken();
      switch (field) {
        case FIELD_SOURCE -> sourceSystem = parser.getValueAsString();
        case FIELD_TARGET -> targetSystem = parser.getValueAsString();
        case FIELD_ELEMENT -> flattenElements(parser, group);
        default -> parser.skipChildren();
      }
    }
    staging.appendGroup(sourceSystem, targetSystem);
  }

  private void flattenElements(@Nonnull final JsonParser parser, final int group)
      throws IOException {
    if (parser.currentToken() != JsonToken.START_ARRAY) {
      parser.skipChildren();
      return;
    }
    while (parser.nextToken() != JsonToken.END_ARRAY) {
      flattenElement(parser, group);
    }
  }

  /**
   * Flattens one element. Its targets may precede its code, so they are held until the element
   * ends, which bounds the memory by the size of the element rather than of the map.
   */
  private void flattenElement(@Nonnull final JsonParser parser, final int group)
      throws IOException {
    if (parser.currentToken() != JsonToken.START_OBJECT) {
      parser.skipChildren();
      return;
    }
    String code = null;
    final List<String[]> targets = new ArrayList<>();
    while (parser.nextToken() == JsonToken.FIELD_NAME) {
      final String field = parser.currentName();
      parser.nextToken();
      switch (field) {
        case FIELD_CODE -> code = parser.getValueAsString();
        case FIELD_TARGET -> readTargets(parser, targets);
        default -> parser.skipChildren();
      }
    }
    if (code == null) {
      return;
    }
    for (final String[] target : targets) {
      staging.appendMapping(group, code, target[0], target[1]);
      if (staging.mappingCount() % PROGRESS_INTERVAL == 0) {
        log.info("Parsed {} mappings", staging.mappingCount());
      }
    }
  }

  /** Reads an element's targets as {@code [code, equivalence]} pairs. */
  private static void readTargets(
      @Nonnull final JsonParser parser, @Nonnull final List<String[]> targets) throws IOException {
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
      String equivalence = null;
      while (parser.nextToken() == JsonToken.FIELD_NAME) {
        final String field = parser.currentName();
        parser.nextToken();
        switch (field) {
          case FIELD_CODE -> code = parser.getValueAsString();
          case FIELD_EQUIVALENCE -> equivalence = parser.getValueAsString();
          default -> parser.skipChildren();
        }
      }
      targets.add(new String[] {code, validEquivalence(equivalence)});
    }
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
}
