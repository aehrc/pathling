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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import jakarta.annotation.Nonnull;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Period;

/**
 * Factory for creating Gson instances configured for SQL on FHIR view definition parsing and for
 * serialising query result rows.
 *
 * @author John Grimes
 */
public final class ViewDefinitionGson {

  private ViewDefinitionGson() {}

  /**
   * Creates a Gson instance configured with adapters for ViewDefinition parsing.
   *
   * <p>The same instance serialises query result rows, so it also carries adapters for the {@code
   * java.time} types that Spark materialises into those rows. Without them Gson falls back to
   * reflection over the private fields of those types, which the JPMS blocks. See {@link
   * Iso8601StringAdapter}.
   *
   * @return a configured Gson instance
   */
  @Nonnull
  public static Gson create() {
    return new GsonBuilder()
        .registerTypeAdapterFactory(new ConstantDeclarationTypeAdapterFactory())
        .registerTypeAdapter(LocalDateTime.class, new Iso8601StringAdapter<LocalDateTime>())
        .registerTypeAdapter(Duration.class, new Iso8601StringAdapter<Duration>())
        .registerTypeAdapter(Period.class, new Iso8601StringAdapter<Period>())
        .registerTypeAdapter(LocalDate.class, new Iso8601StringAdapter<LocalDate>())
        .registerTypeAdapter(Instant.class, new Iso8601StringAdapter<Instant>())
        .disableHtmlEscaping()
        .create();
  }
}
