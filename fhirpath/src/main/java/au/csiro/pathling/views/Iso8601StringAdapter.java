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

package au.csiro.pathling.views;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;

/**
 * Serialises a value as its ISO-8601 string form, using the value's own {@code toString()}.
 *
 * <p>This exists for the {@code java.time} types that Spark materialises into query result rows -
 * {@link java.time.LocalDateTime} for {@code TIMESTAMP_NTZ}, {@link java.time.Duration} for
 * day-time intervals, {@link java.time.Period} for year-month intervals, and {@link
 * java.time.LocalDate} and {@link java.time.Instant} for {@code DATE} and {@code TIMESTAMP} when
 * Spark's Java 8 datetime API is enabled. Gson has no built-in adapter for any of them and falls
 * back to reflection over their private fields, which the JPMS blocks because {@code java.base}
 * does not open {@code java.time}. That surfaced as an opaque HTTP 500 from the operations that
 * stream tabular results.
 *
 * <p>The {@code toString()} form of each of these types is its canonical ISO-8601 representation,
 * and is the string the {@code csv} output already emits, so registering this adapter makes the
 * {@code ndjson}, {@code json} and {@code csv} outputs agree on one representation per value.
 *
 * <p>This adapter is serialisation-only. Nothing parsed through this Gson instance contains {@code
 * java.time} values, so {@link #read} throws rather than shipping untested parsing logic.
 *
 * @param <T> the type being serialised
 * @author John Grimes
 */
public final class Iso8601StringAdapter<T> extends TypeAdapter<T> {

  @Override
  public void write(@Nonnull final JsonWriter out, @Nullable final T value) throws IOException {
    if (value == null) {
      out.nullValue();
    } else {
      out.value(value.toString());
    }
  }

  /**
   * Always throws: this adapter is registered for serialisation only.
   *
   * @param in the reader positioned at the value
   * @return never returns
   * @throws UnsupportedOperationException always
   */
  @Override
  public T read(@Nonnull final JsonReader in) {
    throw new UnsupportedOperationException(
        "Iso8601StringAdapter is serialisation-only; no parse path encounters java.time values");
  }
}
