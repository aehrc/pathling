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
package au.csiro.pathling.legacy;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Value;

/**
 * The arguments of the command line: a source, a format and an output, and optionally the resource
 * type the source holds.
 *
 * @author Piotr Szul
 */
@Value
class LegacyExportArguments {

  /** The option naming the resource type a single table holds. */
  @Nonnull static final String RESOURCE_TYPE_OPTION = "--resource-type";

  /** The location of a table, or of a directory of tables. */
  @Nonnull String source;

  /** The format the tables are stored in. */
  @Nonnull LegacyTableFormat format;

  /** The directory to write into. */
  @Nonnull String output;

  /** The resource type the source holds, where it is named rather than taken from the source. */
  @Getter(AccessLevel.NONE)
  @Nullable
  String resourceType;

  /**
   * Returns the resource type the source holds, where it is named.
   *
   * @return the resource type, or nothing where the source is to name it
   */
  @Nonnull
  Optional<String> getNamedResourceType() {
    return Optional.ofNullable(resourceType);
  }

  /**
   * Parses the arguments of the command line.
   *
   * @param args the arguments
   * @return the parsed arguments
   * @throws IllegalArgumentException if the arguments are not as the usage describes
   */
  @Nonnull
  static LegacyExportArguments parse(@Nonnull final String[] args) {
    final List<String> positional = new ArrayList<>();
    String resourceType = null;
    final Iterator<String> remaining = Arrays.asList(args).iterator();
    while (remaining.hasNext()) {
      final String arg = remaining.next();
      if (RESOURCE_TYPE_OPTION.equals(arg)) {
        if (!remaining.hasNext()) {
          throw new IllegalArgumentException(RESOURCE_TYPE_OPTION + " needs a resource type.");
        }
        resourceType = remaining.next();
      } else if (arg.startsWith("--")) {
        throw new IllegalArgumentException("Unknown option: " + arg + ".");
      } else {
        positional.add(arg);
      }
    }
    if (positional.size() != 3) {
      throw new IllegalArgumentException(
          "Expected a source, a format and an output, but found "
              + positional.size()
              + " arguments.");
    }
    final String formatName = positional.get(1);
    final LegacyTableFormat format =
        LegacyTableFormat.fromName(formatName)
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "Unknown format: "
                            + formatName
                            + ". The format is "
                            + LegacyTableFormat.PARQUET.getName()
                            + " or "
                            + LegacyTableFormat.DELTA.getName()
                            + "."));
    return new LegacyExportArguments(positional.get(0), format, positional.get(2), resourceType);
  }
}
