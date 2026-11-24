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

import au.csiro.pathling.views.FhirView;
import au.csiro.pathling.views.ViewDefinitionGson;
import com.google.gson.Gson;
import jakarta.annotation.Nonnull;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Renders the values that make up an async job cache key, such as SQL text, canonical URLs, labels
 * and parameter values, so that no value can reproduce the delimiters of the key it is placed in.
 *
 * <p>Every value passes through {@link #escape(String)} before it is concatenated, which leaves the
 * delimiters the renderer itself writes as the only structural characters in a description. Two
 * differently structured requests therefore cannot render the same key (aehrc/pathling#2768).
 *
 * @author John Grimes
 */
public final class ContentDescriptions {

  private static final String HEX_DIGITS = "0123456789ABCDEF";

  /** The ASCII characters other than letters and digits that are left as they are. */
  private static final String UNRESERVED_PUNCTUATION = "-._~";

  private static final Gson GSON = ViewDefinitionGson.create();

  private ContentDescriptions() {}

  /**
   * Percent-encodes every ASCII character of a value other than the unreserved characters of RFC
   * 3986 (letters, digits, {@code -}, {@code .}, {@code _} and {@code ~}). Non-ASCII characters are
   * kept, since no delimiter is drawn from them. As {@code %} is itself encoded, the encoding is
   * injective: distinct values always encode differently.
   *
   * <p>For example, {@code SELECT * FROM a} is encoded as {@code SELECT%20%2A%20FROM%20a}.
   *
   * @param value the value to encode
   * @return the encoded value, containing no ASCII punctuation other than {@code %} and the
   *     unreserved characters
   */
  @Nonnull
  public static String escape(@Nonnull final String value) {
    final StringBuilder escaped = new StringBuilder(value.length());
    for (int i = 0; i < value.length(); i++) {
      final char c = value.charAt(i);
      if (c >= 0x80 || Character.isLetterOrDigit(c) || UNRESERVED_PUNCTUATION.indexOf(c) >= 0) {
        escaped.append(c);
      } else {
        escaped.append('%').append(HEX_DIGITS.charAt(c >> 4)).append(HEX_DIGITS.charAt(c & 0xF));
      }
    }
    return escaped.toString();
  }

  /**
   * Renders a map as {@code {key=value,...}}, ordered by key and with every key and value encoded
   * by {@link #escape(String)}. Values are rendered through {@link String#valueOf(Object)}.
   *
   * <p>For example, a map of {@code lbl} to {@code https://example.org/v1} is rendered as {@code
   * {lbl=https%3A%2F%2Fexample.org%2Fv1}}.
   *
   * @param map the map to render
   * @param <V> the type of the map's values
   * @return the rendered map
   */
  @Nonnull
  public static <V> String describeMap(@Nonnull final Map<String, V> map) {
    return map.entrySet().stream()
        .sorted(Map.Entry.comparingByKey())
        .map(entry -> escape(entry.getKey()) + '=' + escape(String.valueOf(entry.getValue())))
        .collect(Collectors.joining(",", "{", "}"));
  }

  /**
   * Renders a parsed view as its encoded JSON. JSON escapes every string it carries, so unlike the
   * view's {@code toString} a FHIRPath expression or column name cannot reproduce the rendering of
   * another element of the view.
   *
   * @param view the view to render
   * @return the encoded JSON of the view
   */
  @Nonnull
  public static String describeView(@Nonnull final FhirView view) {
    return escape(GSON.toJson(view));
  }
}
