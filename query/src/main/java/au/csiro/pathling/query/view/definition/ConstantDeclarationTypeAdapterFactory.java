package au.csiro.pathling.query.view.definition;

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;
import org.jetbrains.annotations.Nullable;

public class ConstantDeclarationTypeAdapterFactory implements TypeAdapterFactory {

  @Nullable
  @Override
  public <T> TypeAdapter<T> create(@Nullable final Gson gson, @Nullable final TypeToken<T> type) {
    if (type == null || ConstantDeclaration.class != type.getRawType()) {
      return null;
    }
    //noinspection unchecked
    return (TypeAdapter<T>) new ConstantDeclarationTypeAdapter();
  }

}
