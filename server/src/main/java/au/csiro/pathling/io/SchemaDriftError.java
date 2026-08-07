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

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Raised when a request cannot be served because a resource type's Delta table schema and this
 * server's encoders disagree. The message names the affected type where it is known, the direction
 * of the disagreement, the field paths involved, and the available remedies, and is surfaced to API
 * clients through an OperationOutcome.
 *
 * <p>Two forms exist. The single-argument form describes a table that startup found to be behind
 * the encoders and could not migrate. The three-argument form describes a disagreement detected
 * when it bit, and distinguishes the two directions: field paths the encoders require but the table
 * lacks, which are migratable, and field paths the table carries but the encoders do not emit,
 * which are not.
 *
 * <p>Neither form includes raw exception text, warehouse paths, or struct definitions, which is
 * what makes the underlying Delta exception unsuitable to return to a client.
 *
 * @author John Grimes
 */
public class SchemaDriftError extends RuntimeException {

  private static final long serialVersionUID = 1L;

  /**
   * The most field paths any one direction contributes to a message. A struct can carry hundreds of
   * fields, and a response body is not the place to enumerate them all; the remainder is summarised
   * by a count.
   */
  private static final int MAX_REPORTED_PATHS = 10;

  @Nullable private final String resourceCode;

  /**
   * Constructs a new SchemaDriftError for the given resource type.
   *
   * @param resourceCode the resource type whose table is drifted and unmigrated
   */
  public SchemaDriftError(@Nonnull final String resourceCode) {
    super(
        "The stored table for resource type '"
            + resourceCode
            + "' has a schema that is behind this server's encoders and cannot be queried. "
            + "Enable pathling.storage.schemaAutoMerge (or restore write access to the "
            + "warehouse) and restart, or update a resource of this type, to migrate the table.");
    this.resourceCode = resourceCode;
  }

  /**
   * Constructs a new SchemaDriftError describing a disagreement between a stored table and this
   * server's encoders, in one or both directions.
   *
   * @param resourceCode the resource type the table holds, or null where it is not known
   * @param missingFieldPaths the field paths the encoders require but the table lacks
   * @param excessFieldPaths the field paths the table carries but the encoders do not emit
   */
  public SchemaDriftError(
      @Nullable final String resourceCode,
      @Nonnull final Collection<String> missingFieldPaths,
      @Nonnull final Collection<String> excessFieldPaths) {
    super(buildMessage(resourceCode, missingFieldPaths, excessFieldPaths));
    this.resourceCode = resourceCode;
  }

  /**
   * Returns the resource type whose table is drifted.
   *
   * @return the resource type code, or null where it was not known
   */
  @Nullable
  public String getResourceCode() {
    return resourceCode;
  }

  /**
   * Assembles the message for the two-direction form. Where neither direction yielded field paths -
   * a recognised condition whose detail could not be interpreted - the condition is still named, so
   * that the caller learns what kind of problem they have.
   */
  @Nonnull
  private static String buildMessage(
      @Nullable final String resourceCode,
      @Nonnull final Collection<String> missingFieldPaths,
      @Nonnull final Collection<String> excessFieldPaths) {
    final String subject =
        resourceCode == null
            ? "A stored table"
            : "The stored table for resource type '" + resourceCode + "'";

    if (missingFieldPaths.isEmpty() && excessFieldPaths.isEmpty()) {
      return subject
          + " cannot be reconciled with this server's encoders. Compare the encoding configuration "
          + "against the configuration the table was written with, in particular "
          + "pathling.encoding.openTypes, and enable pathling.storage.schemaAutoMerge if the table "
          + "is behind the encoders.";
    }

    final Stream<String> clauses =
        Stream.of(
                missingFieldPaths.isEmpty()
                    ? null
                    : " is missing fields that this server's encoders require ("
                        + summarise(missingFieldPaths)
                        + "). Enable pathling.storage.schemaAutoMerge and restart, or update a "
                        + "resource of this type with the flag enabled, to migrate the table.",
                excessFieldPaths.isEmpty()
                    ? null
                    : " carries fields that this server's encoders do not emit ("
                        + summarise(excessFieldPaths)
                        + "). These cannot be reconstructed: restore the encoding configuration the"
                        + " table was written with, in particular pathling.encoding.openTypes, or"
                        + " re-import the data at this server's schema.")
            .filter(Objects::nonNull);

    return subject + clauses.collect(Collectors.joining(" It also"));
  }

  /** Renders a bounded, comma-separated list of field paths. */
  @Nonnull
  private static String summarise(@Nonnull final Collection<String> fieldPaths) {
    final String reported =
        fieldPaths.stream().limit(MAX_REPORTED_PATHS).collect(Collectors.joining(", "));
    final int remainder = fieldPaths.size() - MAX_REPORTED_PATHS;
    return remainder > 0 ? reported + ", and " + remainder + " more" : reported;
  }
}
