package au.csiro.pathling.views;

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;
import javax.annotation.Nullable;

public class SelectClauseTypeAdapterFactory implements TypeAdapterFactory {

  @Nullable
  @Override
  public <T> TypeAdapter<T> create(final Gson gson, final TypeToken<T> type) {
    if (SelectClause.class != type.getRawType()) {
      return null;
    }
    //noinspection unchecked
    return (TypeAdapter<T>) new SelectClauseTypeAdapter(gson);
  }

}
