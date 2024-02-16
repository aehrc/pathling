/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.export.fhir;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import java.time.Instant;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class JsonSupport {

  private JsonSupport() {
  }

  private static final Gson GSON = new GsonBuilder()
      .registerTypeAdapter(Instant.class, new InstantDeserializer())
      .create();

  @Nonnull
  public static <T> Optional<T> fromJson(@Nonnull final String json,
      @Nonnull final Class<T> clazz) {
    try {
      // TODO: also add  JRS-380 based validation
      return Optional.ofNullable(GSON.fromJson(json, clazz));
    } catch (final JsonSyntaxException ex) {
      log.debug("Ignoring invalid JSON parsing error: {}", ex.getMessage());
      return Optional.empty();
    }
  }

  @Nonnull
  public static String toJson(@Nonnull final Object obj) {
    return GSON.toJson(obj);
  }

}
