package au.csiro.pathling.views;

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;
import jakarta.annotation.Nullable;

public class SelectClauseTypeAdapterFactory implements TypeAdapterFactory {

  @Nullable
  @Override
  public <T> TypeAdapter<T> create(@Nullable final Gson gson, @Nullable final TypeToken<T> type) {
    if (gson == null || type == null || SelectClause.class != type.getRawType()) {
      return null;
    }
    //noinspection unchecked
    return (TypeAdapter<T>) new SelectClauseTypeAdapter(gson);
  }

}
