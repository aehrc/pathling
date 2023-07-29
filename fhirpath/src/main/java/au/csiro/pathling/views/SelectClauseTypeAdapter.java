package au.csiro.pathling.views;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.TypeAdapter;
import com.google.gson.internal.Streams;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import javax.annotation.Nonnull;

/**
 * This adapter is used to deserialize a {@link SelectClause} from JSON. It takes a look at the
 * structure and decides which type of clause it is, then delegates deserialization to the
 * appropriate adapter.
 *
 * @author John Grimes
 */
public class SelectClauseTypeAdapter extends TypeAdapter<SelectClause> {

  @Nonnull
  private final Gson gson;

  public SelectClauseTypeAdapter(@Nonnull final Gson gson) {
    this.gson = gson;
  }

  @Override
  public void write(final JsonWriter out, final SelectClause value) throws IOException {
    throw new UnsupportedOperationException("This adapter does not support writing");
  }

  @Override
  public SelectClause read(final JsonReader in) throws IOException {
    final JsonElement jsonElement = Streams.parse(in);
    final JsonObject jsonObject = jsonElement.getAsJsonObject();

    if (jsonObject.has("name")) {
      return gson.fromJson(jsonObject, DirectSelection.class);
    } else if (jsonObject.has("from")) {
      return gson.fromJson(jsonObject, FromSelection.class);
    } else if (jsonObject.has("forEach")) {
      return gson.fromJson(jsonObject, ForEachSelection.class);
    } else {
      throw new JsonParseException(
          "Select clause must contain either 'name', 'from', or 'forEach'");
    }
  }

}
