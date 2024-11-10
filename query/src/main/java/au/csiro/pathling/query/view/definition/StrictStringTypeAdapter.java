package au.csiro.pathling.query.view.definition;

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
