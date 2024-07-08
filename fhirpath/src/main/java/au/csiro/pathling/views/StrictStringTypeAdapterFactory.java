package au.csiro.pathling.views;

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;
import jakarta.annotation.Nullable;

/**
 * This adapter is used to serialize strings, but throw an error if a non-string is encountered.
 *
 * @author John Grimes
 */
public class StrictStringTypeAdapterFactory implements TypeAdapterFactory {

  @Nullable
  @Override
  public <T> TypeAdapter<T> create(@Nullable final Gson gson, @Nullable final TypeToken<T> type) {
    if (gson == null || type == null || String.class != type.getRawType()) {
      return null;
    }
    //noinspection unchecked
    return (TypeAdapter<T>) new StrictStringTypeAdapter();
  }

}
