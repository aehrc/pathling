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

package au.csiro.pathling.operations.sqlquery;

import au.csiro.pathling.terminology.conceptmap.ConceptMapContent;
import au.csiro.pathling.terminology.conceptmap.ConceptMapping;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.HexFormat;
import lombok.Value;

/**
 * A resolved leaf node for a concept map dependency. The node carries the mappings of the concept
 * map as they were fixed for the job, from an inline {@code context} resource or from the
 * configured terminology layer, and is exposed to the SQL as a nine-column relation. A concept map
 * declares no further dependencies, so it is always a leaf of the dependency graph.
 *
 * @author John Grimes
 */
@Value
public class ResolvedConceptMap implements ResolvedDependency {

  /** Separates the columns of one mapping within the hashed content description. */
  private static final char COLUMN_SEPARATOR = '|';

  /** Terminates each mapping within the hashed content description. */
  private static final char ROW_SEPARATOR = '\n';

  /** Stands in for a null column value within the hashed content description. */
  private static final String NULL_VALUE = "\u0000";

  /** Orders nullable columns with null before any value. */
  private static final Comparator<String> NULLS_FIRST =
      Comparator.nullsFirst(Comparator.naturalOrder());

  /**
   * Sorts mappings by their seven identity columns so that the description is independent of source
   * order.
   */
  private static final Comparator<ConceptMapping> BY_IDENTITY =
      Comparator.comparing(ConceptMapping::getSourceSystem)
          .thenComparing(ConceptMapping::getSourceVersion, NULLS_FIRST)
          .thenComparing(ConceptMapping::getSourceCode)
          .thenComparing(ConceptMapping::getTargetSystem, NULLS_FIRST)
          .thenComparing(ConceptMapping::getTargetVersion, NULLS_FIRST)
          .thenComparing(ConceptMapping::getTargetCode, NULLS_FIRST)
          .thenComparing(ConceptMapping::getRelationship, NULLS_FIRST);

  /**
   * The reference canonical as written in the dependency, {@code url} or {@code url|version}. The
   * key is the reference rather than the version the terminology layer resolved, so that a second
   * reference to the same string within a job reuses this node without a second lookup.
   */
  @Nonnull String canonicalKey;

  /** The mappings and the version resolved, fixed for the life of the job. */
  @Nonnull ConceptMapContent content;

  /**
   * A concept map's rows are exactly its mappings, so the description is a digest of every
   * mapping's nine columns in identity order: the same mappings from any source, in any order,
   * describe themselves identically, and a change to any column of any mapping changes the
   * description. The map's URL and version take no part, since they do not alter the rows.
   *
   * @return the content description
   */
  @Override
  @Nonnull
  public String describeContent() {
    final MessageDigest digest = sha256();
    content.getMappings().stream()
        .sorted(BY_IDENTITY)
        .forEach(mapping -> digest.update(render(mapping).getBytes(StandardCharsets.UTF_8)));
    return "concept-map:" + HexFormat.of().formatHex(digest.digest());
  }

  /**
   * Renders one mapping as its nine columns, with a sentinel for null.
   *
   * @param mapping the mapping
   * @return the rendered row
   */
  @Nonnull
  private static String render(@Nonnull final ConceptMapping mapping) {
    return mapping.getSourceSystem()
        + COLUMN_SEPARATOR
        + orNull(mapping.getSourceVersion())
        + COLUMN_SEPARATOR
        + mapping.getSourceCode()
        + COLUMN_SEPARATOR
        + orNull(mapping.getSourceDisplay())
        + COLUMN_SEPARATOR
        + orNull(mapping.getTargetSystem())
        + COLUMN_SEPARATOR
        + orNull(mapping.getTargetVersion())
        + COLUMN_SEPARATOR
        + orNull(mapping.getTargetCode())
        + COLUMN_SEPARATOR
        + orNull(mapping.getTargetDisplay())
        + COLUMN_SEPARATOR
        + orNull(mapping.getRelationship())
        + ROW_SEPARATOR;
  }

  /**
   * Renders a nullable column value, substituting the null sentinel.
   *
   * @param value the value
   * @return the rendered value
   */
  @Nonnull
  private static String orNull(@Nullable final String value) {
    return value == null ? NULL_VALUE : value;
  }

  /**
   * Obtains a SHA-256 digest, which every Java platform is required to provide.
   *
   * @return a fresh digest
   */
  @Nonnull
  private static MessageDigest sha256() {
    try {
      return MessageDigest.getInstance("SHA-256");
    } catch (final NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is not available", e);
    }
  }
}
