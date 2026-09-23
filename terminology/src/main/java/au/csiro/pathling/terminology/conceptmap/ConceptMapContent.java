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

package au.csiro.pathling.terminology.conceptmap;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.Serial;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.Value;
import org.hl7.fhir.r4.model.ConceptMap;
import org.hl7.fhir.r4.model.ConceptMap.ConceptMapGroupComponent;
import org.hl7.fhir.r4.model.ConceptMap.SourceElementComponent;
import org.hl7.fhir.r4.model.ConceptMap.TargetElementComponent;
import org.hl7.fhir.r4.model.Enumerations.ConceptMapEquivalence;

/**
 * The mappings of one concept map at one resolution: the canonical URL, the version resolved where
 * known, and the deduplicated rows in first-seen order.
 *
 * <p>Every source of a concept map converges on this shape through one of two factories. {@link
 * #fromResource} converts an R4 ConceptMap, applying the official R4 to R5 conversion of
 * equivalences and rejecting content that the flat rows of a concept map relation cannot carry;
 * {@link #fromMappings} deduplicates rows that a source has already built. Both apply the caller's
 * cap once the rows are complete.
 *
 * @author John Grimes
 */
@Value
public class ConceptMapContent implements Serializable {

  @Serial private static final long serialVersionUID = -8297641350726318852L;

  /** Separates a code system URI from its version within a canonical reference. */
  private static final char VERSION_SEPARATOR = '|';

  /** The canonical URL of the concept map. */
  @Nonnull String url;

  /** The version of the concept map that was resolved, or null where none is known. */
  @Nullable String version;

  /** The mappings, deduplicated on identity and in first-seen order; may be empty. */
  @Nonnull List<ConceptMapping> mappings;

  /**
   * Converts an R4 ConceptMap resource to the rows of a concept map relation, walking {@code
   * group}, {@code element} and {@code target} in document order. Each target whose equivalence is
   * not {@code unmatched} is one row; each element with at least one {@code unmatched} target is
   * one no-mapping row with a null target code, display and relationship. {@code group.unmapped} is
   * ignored and an element without targets contributes nothing. The whole resource is walked before
   * the cap is applied, so a fault is reported even where it lies beyond the cap.
   *
   * @param conceptMap the resource
   * @param maxMappings the largest number of rows the caller will accept
   * @return the content
   * @throws IllegalArgumentException if the resource carries no {@code url}
   * @throws ConceptMapContentException if a group has no source, a target carries {@code dependsOn}
   *     or {@code product}, an element has no code, a target whose equivalence is not {@code
   *     unmatched} has no code, or a target has no equivalence
   * @throws ConceptMapLimitExceededException if the deduplicated rows exceed {@code maxMappings}
   */
  @Nonnull
  public static ConceptMapContent fromResource(
      @Nonnull final ConceptMap conceptMap, final int maxMappings) {
    final String url = valueOf(conceptMap.getUrl());
    if (url == null) {
      throw new IllegalArgumentException("The concept map carries no url");
    }
    final List<ConceptMapping> rows = new ArrayList<>();
    for (final ConceptMapGroupComponent group : conceptMap.getGroup()) {
      addGroup(group, rows);
    }
    return fromMappings(url, valueOf(conceptMap.getVersion()), rows, maxMappings);
  }

  /**
   * Builds the content of a concept map from rows, deduplicating on identity and keeping the first
   * occurrence of each.
   *
   * @param url the canonical URL of the concept map
   * @param version the version resolved, or null where none is known
   * @param mappings the rows, in the order they were produced
   * @param maxMappings the largest number of rows the caller will accept
   * @return the content
   * @throws ConceptMapLimitExceededException once the retained rows exceed {@code maxMappings}; the
   *     rest of {@code mappings} is not consumed
   */
  @Nonnull
  public static ConceptMapContent fromMappings(
      @Nonnull final String url,
      @Nullable final String version,
      @Nonnull final Iterable<ConceptMapping> mappings,
      final int maxMappings) {
    final Map<String, ConceptMapping> retained = new LinkedHashMap<>();
    for (final ConceptMapping mapping : mappings) {
      if (retained.putIfAbsent(mapping.identity(), mapping) == null
          && retained.size() > maxMappings) {
        throw new ConceptMapLimitExceededException(maxMappings);
      }
    }
    return new ConceptMapContent(
        url, version, Collections.unmodifiableList(new ArrayList<>(retained.values())));
  }

  /** Appends the rows of one group, in document order. */
  private static void addGroup(
      @Nonnull final ConceptMapGroupComponent group, @Nonnull final List<ConceptMapping> rows) {
    // Each primitive is tested by its value, as HAPI reports an element that carries only an
    // extension as present while giving it no value.
    final String source = valueOf(group.getSource());
    if (source == null) {
      throw new ConceptMapContentException("a group has no source system");
    }
    final String explicitSourceVersion = valueOf(group.getSourceVersion());
    final String sourceSystem;
    final String sourceVersion;
    if (explicitSourceVersion != null) {
      sourceSystem = source;
      sourceVersion = explicitSourceVersion;
    } else {
      sourceSystem = systemOf(source);
      sourceVersion = versionOf(source);
    }
    final String targetUri = valueOf(group.getTarget());
    final String explicitTargetVersion = valueOf(group.getTargetVersion());
    final String targetSystem;
    final String targetVersion;
    if (targetUri == null) {
      targetSystem = null;
      targetVersion = null;
    } else if (explicitTargetVersion != null) {
      targetSystem = targetUri;
      targetVersion = explicitTargetVersion;
    } else {
      targetSystem = systemOf(targetUri);
      targetVersion = versionOf(targetUri);
    }
    for (final SourceElementComponent element : group.getElement()) {
      final String sourceCode = valueOf(element.getCode());
      if (sourceCode == null) {
        throw new ConceptMapContentException("an element has no code");
      }
      final String sourceDisplay = valueOf(element.getDisplay());
      for (final TargetElementComponent target : element.getTarget()) {
        rows.add(
            row(
                sourceSystem,
                sourceVersion,
                sourceCode,
                sourceDisplay,
                targetSystem,
                targetVersion,
                target));
      }
    }
  }

  /** Builds the row for one target of an element. */
  @Nonnull
  private static ConceptMapping row(
      @Nonnull final String sourceSystem,
      @Nullable final String sourceVersion,
      @Nonnull final String sourceCode,
      @Nullable final String sourceDisplay,
      @Nullable final String targetSystem,
      @Nullable final String targetVersion,
      @Nonnull final TargetElementComponent target) {
    if (target.hasDependsOn()) {
      throw new ConceptMapContentException(
          "the mapping for source code '" + sourceCode + "' depends on other elements (dependsOn)");
    }
    if (target.hasProduct()) {
      throw new ConceptMapContentException(
          "the mapping for source code '" + sourceCode + "' has products");
    }
    final ConceptMapEquivalence equivalence = target.getEquivalence();
    if (equivalence == null) {
      throw new ConceptMapContentException(
          "the mapping for source code '" + sourceCode + "' has no equivalence");
    }
    if (equivalence == ConceptMapEquivalence.UNMATCHED) {
      // The map states that the source code has no mapping; any code the target carries is not
      // exposed, as in the official R4 to R5 conversion.
      return new ConceptMapping(
          sourceSystem,
          sourceVersion,
          sourceCode,
          sourceDisplay,
          targetSystem,
          targetVersion,
          null,
          null,
          null);
    }
    final String targetCode = valueOf(target.getCode());
    if (targetCode == null) {
      throw new ConceptMapContentException(
          "the mapping for source code '" + sourceCode + "' has no target code");
    }
    return new ConceptMapping(
        sourceSystem,
        sourceVersion,
        sourceCode,
        sourceDisplay,
        targetSystem,
        targetVersion,
        targetCode,
        valueOf(target.getDisplay()),
        ConceptMapRelationship.of(equivalence));
  }

  /**
   * Returns the value of a string primitive, or null where it has none or only whitespace, which is
   * what HAPI reports as absent.
   */
  @Nullable
  private static String valueOf(@Nullable final String value) {
    return value == null || value.isBlank() ? null : value;
  }

  /** Returns the part of a canonical reference before its first {@code |}, or all of it. */
  @Nonnull
  private static String systemOf(@Nonnull final String canonical) {
    final int pipe = canonical.indexOf(VERSION_SEPARATOR);
    return pipe < 0 ? canonical : canonical.substring(0, pipe);
  }

  /** Returns the part of a canonical reference after its first {@code |}, or null if none. */
  @Nullable
  private static String versionOf(@Nonnull final String canonical) {
    final int pipe = canonical.indexOf(VERSION_SEPARATOR);
    return pipe < 0 ? null : canonical.substring(pipe + 1);
  }
}
