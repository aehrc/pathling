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
    if (!conceptMap.hasUrl()) {
      throw new IllegalArgumentException("The concept map carries no url");
    }
    final List<ConceptMapping> rows = new ArrayList<>();
    for (final ConceptMapGroupComponent group : conceptMap.getGroup()) {
      addGroup(group, rows);
    }
    return fromMappings(
        conceptMap.getUrl(),
        conceptMap.hasVersion() ? conceptMap.getVersion() : null,
        rows,
        maxMappings);
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
    if (!group.hasSource()) {
      throw new ConceptMapContentException("a group has no source system");
    }
    final String sourceSystem;
    final String sourceVersion;
    if (group.hasSourceVersion()) {
      sourceSystem = group.getSource();
      sourceVersion = group.getSourceVersion();
    } else {
      sourceSystem = systemOf(group.getSource());
      sourceVersion = versionOf(group.getSource());
    }
    final String targetSystem;
    final String targetVersion;
    if (!group.hasTarget()) {
      targetSystem = null;
      targetVersion = null;
    } else if (group.hasTargetVersion()) {
      targetSystem = group.getTarget();
      targetVersion = group.getTargetVersion();
    } else {
      targetSystem = systemOf(group.getTarget());
      targetVersion = versionOf(group.getTarget());
    }
    for (final SourceElementComponent element : group.getElement()) {
      if (!element.hasCode()) {
        throw new ConceptMapContentException("an element has no code");
      }
      final String sourceCode = element.getCode();
      final String sourceDisplay = element.hasDisplay() ? element.getDisplay() : null;
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
    if (!target.hasEquivalence()) {
      throw new ConceptMapContentException(
          "the mapping for source code '" + sourceCode + "' has no equivalence");
    }
    if (target.getEquivalence() == ConceptMapEquivalence.UNMATCHED) {
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
    if (!target.hasCode()) {
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
        target.getCode(),
        target.hasDisplay() ? target.getDisplay() : null,
        ConceptMapRelationship.of(target.getEquivalence()));
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
