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

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import java.lang.reflect.Type;
import java.time.Instant;

public class InstantDeserializer implements JsonDeserializer<Instant> {

  @Override
  public Instant deserialize(final JsonElement json, final Type typeOfT,
      final JsonDeserializationContext context) throws JsonParseException {
    if (!json.isJsonPrimitive()) {
      throw new JsonParseException("Primitive expected  but got " + json);
    }
    final JsonPrimitive primitive = json.getAsJsonPrimitive();
    try {
      return Instant.ofEpochMilli(primitive.getAsLong());
    } catch (final NumberFormatException ex) {
      return Instant.parse(primitive.getAsString());
    }
  }
}

