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

package au.csiro.pathling.terminology.local;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.IntStream;

/**
 * Reads a weighted language preference list into the dialects to try, in the order to try them, as
 * <a href="https://www.rfc-editor.org/rfc/rfc9110#field.accept-language">RFC 9110</a> describes.
 *
 * <p>Entries are separated by commas and may each carry a {@code ;q=} weight, which defaults to 1.
 * An entry given zero weight is excluded, as is the {@code *} wildcard, which accepts anything and
 * so is indistinguishable in effect from expressing no preference. Surviving entries are ordered by
 * descending weight, with ties keeping the order they were written.
 *
 * <p>Nothing here raises an error: a value that cannot be read yields an empty list, which behaves
 * as no preference. A lookup failing over a quirk in a header would be out of all proportion, and
 * remote mode, which passes the header to the terminology server, would not fail either.
 *
 * @author John Grimes
 */
public final class LanguagePreference {

  /** The wildcard tag, which accepts any language and so expresses no preference. */
  private static final String WILDCARD = "*";

  /** The parameter carrying an entry's relative weight. */
  private static final String WEIGHT_PARAMETER = "q=";

  /** The weight of an entry whose weight parameter cannot be read, which excludes it. */
  private static final double UNREADABLE = -1;

  private LanguagePreference() {
    // Utility class.
  }

  /**
   * Reads a language preference list into the tags to try.
   *
   * @param headerValue the preference list, which may be null, blank or malformed
   * @return the tags to try, in descending order of weight; empty where no preference is expressed
   */
  @Nonnull
  public static List<String> parse(@Nullable final String headerValue) {
    if (headerValue == null || headerValue.isBlank()) {
      return List.of();
    }
    final List<String> tags = new ArrayList<>();
    final List<Double> weights = new ArrayList<>();
    for (final String entry : headerValue.split(",", -1)) {
      final String[] parts = entry.split(";");
      // An entry of nothing but parameter separators splits to nothing at all.
      if (parts.length == 0) {
        continue;
      }
      final String tag = parts[0].trim();
      if (tag.isEmpty() || WILDCARD.equals(tag)) {
        continue;
      }
      final double weight = readWeight(parts);
      if (weight > 0) {
        tags.add(tag);
        weights.add(weight);
      }
    }
    // Sorting the positions rather than the tags, with a stable sort over positions already in the
    // order they were written, leaves entries of equal weight in that order.
    final List<Integer> byWeight =
        new ArrayList<>(IntStream.range(0, tags.size()).boxed().toList());
    byWeight.sort(
        Comparator.comparingDouble((Integer position) -> weights.get(position)).reversed());
    return byWeight.stream().map(tags::get).toList();
  }

  /**
   * Reads the weight of an entry from its parameters.
   *
   * @param parts the entry split on its parameter separator, the tag first
   * @return the weight, 1 where no weight parameter is present, or {@link #UNREADABLE} where one is
   *     present but is not a number between 0 and 1
   */
  private static double readWeight(@Nonnull final String[] parts) {
    for (int index = 1; index < parts.length; index++) {
      final String parameter = parts[index].trim();
      if (!parameter.regionMatches(true, 0, WEIGHT_PARAMETER, 0, WEIGHT_PARAMETER.length())) {
        continue;
      }
      try {
        final double weight = Double.parseDouble(parameter.substring(WEIGHT_PARAMETER.length()));
        return weight >= 0 && weight <= 1 ? weight : UNREADABLE;
      } catch (final NumberFormatException e) {
        return UNREADABLE;
      }
    }
    return 1;
  }
}
