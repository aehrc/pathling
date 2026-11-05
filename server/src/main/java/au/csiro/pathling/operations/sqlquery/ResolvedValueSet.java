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

import au.csiro.pathling.terminology.expand.ValueSetExpansion;
import au.csiro.pathling.terminology.expand.ValueSetMember;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.HexFormat;
import lombok.Value;

/**
 * A resolved leaf node for a value set dependency. The node carries the membership of the value set
 * as it was fixed for the job, from an inline {@code context} resource or from the configured
 * terminology layer, and is exposed to the SQL as a five-column relation. A value set declares no
 * further dependencies, so it is always a leaf of the dependency graph.
 *
 * @author John Grimes
 */
@Value
public class ResolvedValueSet implements ResolvedDependency {

  /** Separates the columns of one member within the hashed content description. */
  private static final char COLUMN_SEPARATOR = '|';

  /** Terminates each member within the hashed content description. */
  private static final char ROW_SEPARATOR = '\n';

  /** Stands in for a null column value within the hashed content description. */
  private static final String NULL_VALUE = "\u0000";

  /** Sorts members by their identity so that the description is independent of source order. */
  private static final Comparator<ValueSetMember> BY_IDENTITY =
      Comparator.comparing(ValueSetMember::getSystem)
          .thenComparing(
              ValueSetMember::getVersion, Comparator.nullsFirst(Comparator.naturalOrder()))
          .thenComparing(ValueSetMember::getCode);

  /**
   * The reference canonical as written in the dependency, {@code url} or {@code url|version}. The
   * key is the reference rather than the version the terminology layer resolved, so that a second
   * reference to the same string within a job reuses this node without a second expansion.
   */
  @Nonnull String canonicalKey;

  /** The membership and its provenance, fixed for the life of the job. */
  @Nonnull ValueSetExpansion expansion;

  /**
   * A value set's rows are exactly its members, so the description is a digest of every member's
   * five columns in identity order: the same membership from any source, in any order, describes
   * itself identically, and a change to any column of any member changes the description.
   * Provenance takes no part, since it does not alter the rows.
   *
   * @return the content description
   */
  @Override
  @Nonnull
  public String describeContent() {
    final MessageDigest digest = sha256();
    expansion.getMembers().stream()
        .sorted(BY_IDENTITY)
        .forEach(member -> digest.update(render(member).getBytes(StandardCharsets.UTF_8)));
    return "value-set:" + HexFormat.of().formatHex(digest.digest());
  }

  /**
   * Renders one member as its five columns, with a sentinel for null.
   *
   * @param member the member
   * @return the rendered row
   */
  @Nonnull
  private static String render(@Nonnull final ValueSetMember member) {
    return member.getSystem()
        + COLUMN_SEPARATOR
        + orNull(member.getVersion())
        + COLUMN_SEPARATOR
        + member.getCode()
        + COLUMN_SEPARATOR
        + orNull(member.getDisplay())
        + COLUMN_SEPARATOR
        + orNull(member.getInactive())
        + ROW_SEPARATOR;
  }

  /**
   * Renders a nullable column value, substituting the null sentinel.
   *
   * @param value the value
   * @return the rendered value
   */
  @Nonnull
  private static String orNull(@Nullable final Object value) {
    return value == null ? NULL_VALUE : value.toString();
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
