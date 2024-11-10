package au.csiro.pathling.query.view.definition;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.TypeAdapter;
import com.google.gson.internal.Streams;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import org.jetbrains.annotations.NotNull;

/**
 * This adapter is used to deserialize a {@link SelectClause} from JSON. It takes a look at the
 * structure and decides which type of clause it is, then delegates deserialization to the
 * appropriate adapter.
 *
 * @author John Grimes
 */
public class SelectClauseTypeAdapter extends TypeAdapter<SelectClause> {

  @NotNull
  private final Gson gson;

  public SelectClauseTypeAdapter(@NotNull final Gson gson) {
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

    if (jsonObject.has("forEach")) {
      return gson.fromJson(jsonObject, ForEachSelect.class);
    } else if (jsonObject.has("forEachOrNull")) {
      return gson.fromJson(jsonObject, ForEachOrNullSelect.class);
    } else {
      return gson.fromJson(jsonObject, ColumnSelect.class);
    }
  }
}
