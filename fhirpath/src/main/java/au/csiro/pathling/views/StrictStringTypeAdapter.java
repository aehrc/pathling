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

import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.TypeAdapter;
import com.google.gson.internal.Streams;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;

/**
 * This is a factory to create an adapter that is used to serialize strings, but throw an error if a
 * non-string is encountered.
 *
 * @author John Grimes
 */
public class StrictStringTypeAdapter extends TypeAdapter<String> {

  @Override
  public void write(final JsonWriter out, final String value) throws IOException {
    throw new UnsupportedOperationException("This adapter does not support writing");
  }

  @Override
  public String read(final JsonReader in) throws IOException {
    final JsonElement jsonElement = Streams.parse(in);
    if (!jsonElement.isJsonPrimitive() || !jsonElement.getAsJsonPrimitive().isString()) {
      throw new JsonParseException(
          "Expected a string but got something else: " + jsonElement.getAsString());
    }
    return jsonElement.getAsString();
  }
}
