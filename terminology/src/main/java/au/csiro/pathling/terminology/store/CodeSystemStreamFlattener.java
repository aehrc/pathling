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
import java.util.HashSet;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

/**
 * Walks the Jackson token stream of a single FHIR CodeSystem, transcoding its concepts,
 * designations, scalar and Coding-valued properties, and nesting-derived is-a edges into staging
 * rows. Dense identifiers are assigned from a running counter in document order (pre-order over the
 * nested concept hierarchy), so the flattener holds only the current concept subtree and the
 * dense-identifier stack of the enclosing concepts, never state proportional to the source size.
 *
 * <p>The flattener accepts any {@link JsonParser}, so a CodeSystem extracted from a Bundle can be
 * re-encoded and fed through exactly the same path as a standalone CodeSystem.
 *
 * @author John Grimes
 */
@Slf4j
public class CodeSystemStreamFlattener {

  /** The default designation-usage system attributed to designations that omit a use system. */
  static final String DESIGNATION_USAGE_SYSTEM =
      "http://terminology.hl7.org/CodeSystem/designation-usage";

  /** The number of concepts between running-count progress messages by default. */
  static final long DEFAULT_PROGRESS_INTERVAL = 1_000_000L;

  private static final String FIELD_RESOURCE_CONCEPT = "concept";
  private static final String FIELD_HIERARCHY_MEANING = "hierarchyMeaning";
  private static final String FIELD_CODE = "code";
  private static final String FIELD_DISPLAY = "display";
  private static final String FIELD_DESIGNATION = "designation";
  private static final String FIELD_PROPERTY = "property";
  private static final String FIELD_VALUE = "value";
  private static final String FIELD_SYSTEM = "system";
  private static final String FIELD_LANGUAGE = "language";
  private static final String FIELD_USE = "use";
  private static final String PROPERTY_INACTIVE = "inactive";
  private static final String CODING_TYPE = "Coding";

  private static final String PROPERTY_PARENT = "parent";
  private static final String PROPERTY_CHILD = "child";
  private static final String STANDARD_PARENT_URI = "http://hl7.org/fhir/concept-properties#parent";
  private static final String STANDARD_CHILD_URI = "http://hl7.org/fhir/concept-properties#child";
  private static final String FIELD_URI = "uri";
  private static final String ROLE_CHILD = "child";
  private static final String ROLE_PARENT = "parent";

  @Nonnull private final CodeSystemStaging staging;

  // Property codes recognised as parent- or child-valued is-a edges. The standard property codes
  // parent and child are always recognised; a declaration carrying the standard URI adds its code.
  @Nonnull private final Set<String> parentPropertyCodes = new HashSet<>(Set.of(PROPERTY_PARENT));
  @Nonnull private final Set<String> childPropertyCodes = new HashSet<>(Set.of(PROPERTY_CHILD));

  @Nullable private String hierarchyMeaning;

  private int nextDenseId;

  /**
   * Creates a flattener that writes into the given staging.
   *
   * @param staging the staging to append rows to
   */
  public CodeSystemStreamFlattener(@Nonnull final CodeSystemStaging staging) {
    this.staging = staging;
  }

  /**
   * Returns the CodeSystem's hierarchy meaning, if one was declared.
   *
   * @return the {@code hierarchyMeaning} code, or null if absent
   */
  @Nullable
  public String getHierarchyMeaning() {
    return hierarchyMeaning;
  }

  /**
   * Flattens a CodeSystem from the current parser position, appending staging rows.
   *
   * @param parser a parser positioned before the CodeSystem object
   * @return the number of concepts flattened (including any duplicate codes)
   * @throws IOException if the stream cannot be read
   */
  public int flatten(@Nonnull final JsonParser parser) throws IOException {
    if (parser.nextToken() != JsonToken.START_OBJECT) {
      throw new TerminologyImportException("Expected a CodeSystem JSON object");
    }
    while (parser.nextToken() == JsonToken.FIELD_NAME) {
      final String field = parser.currentName();
      parser.nextToken();
      switch (field) {
        case FIELD_HIERARCHY_MEANING -> hierarchyMeaning = parser.getValueAsString();
        case FIELD_PROPERTY -> readPropertyDeclarations(parser);
        case FIELD_RESOURCE_CONCEPT -> flattenConceptArray(parser, null);
        default -> parser.skipChildren();
      }
    }
    return nextDenseId;
  }

  /** Flattens a {@code concept} array whose members are children of {@code parentDense}. */
  private void flattenConceptArray(
      @Nonnull final JsonParser parser, @Nullable final Integer parentDense) throws IOException {
    if (parser.currentToken() != JsonToken.START_ARRAY) {
      // A concept field that is not an array carries no concepts.
      parser.skipChildren();
      return;
    }
    while (parser.nextToken() != JsonToken.END_ARRAY) {
      flattenConcept(parser, parentDense);
    }
  }

  /**
   * Flattens a single concept object, assigning its dense identifier and recursing into children.
   */
  private void flattenConcept(@Nonnull final JsonParser parser, @Nullable final Integer parentDense)
      throws IOException {
    final int dense = nextDenseId++;
    if (parentDense != null) {
      // A nested concept is-a its enclosing concept.
      staging.appendDenseEdge(dense, parentDense);
    }

    String code = null;
    String display = null;
    boolean active = true;
    while (parser.nextToken() != JsonToken.END_OBJECT) {
      final String field = parser.currentName();
      parser.nextToken();
      switch (field) {
        case FIELD_CODE -> code = parser.getValueAsString();
        case FIELD_DISPLAY -> display = parser.getValueAsString();
        case FIELD_DESIGNATION -> flattenDesignations(parser, dense);
        case FIELD_PROPERTY -> {
          if (flattenProperties(parser, dense)) {
            active = false;
          }
        }
        case FIELD_RESOURCE_CONCEPT -> flattenConceptArray(parser, dense);
        default -> parser.skipChildren();
      }
    }

    final String resolvedDisplay = display != null ? display : code;
    staging.appendConcept(
        code == null ? "" : code,
        dense,
        active,
        false,
        resolvedDisplay == null ? "" : resolvedDisplay);
  }

  /** Flattens a concept's {@code designation} array into description staging rows. */
  private void flattenDesignations(@Nonnull final JsonParser parser, final int dense)
      throws IOException {
    if (parser.currentToken() != JsonToken.START_ARRAY) {
      parser.skipChildren();
      return;
    }
    while (parser.nextToken() != JsonToken.END_ARRAY) {
      String value = null;
      String language = null;
      String useCode = null;
      String useSystem = null;
      while (parser.nextToken() != JsonToken.END_OBJECT) {
        final String field = parser.currentName();
        parser.nextToken();
        switch (field) {
          case FIELD_VALUE -> value = parser.getValueAsString();
          case FIELD_LANGUAGE -> language = parser.getValueAsString();
          case FIELD_USE -> {
            final String[] use = readCodingSystemAndCode(parser);
            useSystem = use[0];
            useCode = use[1];
          }
          default -> parser.skipChildren();
        }
      }
      staging.appendDescription(
          dense,
          value,
          language,
          useCode,
          useSystem != null ? useSystem : DESIGNATION_USAGE_SYSTEM);
    }
  }

  /**
   * Flattens a concept's {@code property} array into scalar-property and Coding-property staging
   * rows, returning whether an {@code inactive} property marks the concept inactive.
   */
  private boolean flattenProperties(@Nonnull final JsonParser parser, final int dense)
      throws IOException {
    if (parser.currentToken() != JsonToken.START_ARRAY) {
      parser.skipChildren();
      return false;
    }
    boolean inactive = false;
    while (parser.nextToken() != JsonToken.END_ARRAY) {
      String propertyCode = null;
      String valueType = null;
      String scalarValue = null;
      String codingCode = null;
      while (parser.nextToken() != JsonToken.END_OBJECT) {
        final String field = parser.currentName();
        parser.nextToken();
        if (FIELD_CODE.equals(field)) {
          propertyCode = parser.getValueAsString();
        } else if (field.startsWith(FIELD_VALUE) && field.length() > FIELD_VALUE.length()) {
          valueType = fhirTypeOf(field);
          if (CODING_TYPE.equals(valueType)) {
            codingCode = readCodingSystemAndCode(parser)[1];
          } else {
            scalarValue = parser.getValueAsString();
          }
        } else {
          parser.skipChildren();
        }
      }
      if (propertyCode == null) {
        continue;
      }
      final String referencedCode = CODING_TYPE.equals(valueType) ? codingCode : scalarValue;
      if (CODING_TYPE.equals(valueType)) {
        if (codingCode != null) {
          staging.appendCodingProperty(dense, propertyCode, codingCode);
        }
      } else if (scalarValue != null) {
        staging.appendProperty(dense, propertyCode, valueType, scalarValue);
        if (PROPERTY_INACTIVE.equals(propertyCode) && "true".equalsIgnoreCase(scalarValue)) {
          inactive = true;
        }
      }
      // A parent property makes this concept a child of the referenced code; a child property makes
      // it the parent. The property row is retained above; the edge is derived, not moved.
      if (referencedCode != null) {
        if (parentPropertyCodes.contains(propertyCode)) {
          staging.appendCodeEdge(dense, ROLE_CHILD, referencedCode);
        }
        if (childPropertyCodes.contains(propertyCode)) {
          staging.appendCodeEdge(dense, ROLE_PARENT, referencedCode);
        }
      }
    }
    return inactive;
  }

  /**
   * Reads the top-level {@code property} declarations, recognising any property code whose declared
   * URI is the standard parent or child concept-property URI. Declarations that appear before the
   * concepts (as in every real CodeSystem) inform hierarchy detection during flattening.
   */
  private void readPropertyDeclarations(@Nonnull final JsonParser parser) throws IOException {
    if (parser.currentToken() != JsonToken.START_ARRAY) {
      parser.skipChildren();
      return;
    }
    while (parser.nextToken() != JsonToken.END_ARRAY) {
      String code = null;
      String uri = null;
      while (parser.nextToken() != JsonToken.END_OBJECT) {
        final String field = parser.currentName();
        parser.nextToken();
        switch (field) {
          case FIELD_CODE -> code = parser.getValueAsString();
          case FIELD_URI -> uri = parser.getValueAsString();
          default -> parser.skipChildren();
        }
      }
      if (code != null) {
        if (STANDARD_PARENT_URI.equals(uri)) {
          parentPropertyCodes.add(code);
        } else if (STANDARD_CHILD_URI.equals(uri)) {
          childPropertyCodes.add(code);
        }
      }
    }
  }

  /**
   * Reads a Coding object, returning its system and code. The object is fully consumed.
   *
   * @return a two-element array of {@code [system, code]}, either element null when absent
   */
  @Nonnull
  private static String[] readCodingSystemAndCode(@Nonnull final JsonParser parser)
      throws IOException {
    final String[] result = new String[2];
    if (parser.currentToken() != JsonToken.START_OBJECT) {
      parser.skipChildren();
      return result;
    }
    while (parser.nextToken() != JsonToken.END_OBJECT) {
      final String field = parser.currentName();
      parser.nextToken();
      switch (field) {
        case FIELD_SYSTEM -> result[0] = parser.getValueAsString();
        case FIELD_CODE -> result[1] = parser.getValueAsString();
        default -> parser.skipChildren();
      }
    }
    return result;
  }

  /**
   * Derives the FHIR type name from a {@code value[x]} field name, matching HAPI's {@code
   * fhirType()}: primitives are lower-camel-case ({@code valueInteger} to {@code integer}), while
   * {@code Coding} keeps its capital.
   */
  @Nonnull
  private static String fhirTypeOf(@Nonnull final String valueField) {
    final String suffix = valueField.substring(FIELD_VALUE.length());
    if (CODING_TYPE.equals(suffix)) {
      return CODING_TYPE;
    }
    return Character.toLowerCase(suffix.charAt(0)) + suffix.substring(1);
  }
}
