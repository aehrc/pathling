package au.csiro.pathling.views;

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;
import jakarta.annotation.Nullable;

/**
 * Type adapter factory for {@link ConstantDeclaration} serialisation with Gson.
 *
 * @author John Grimes
 */
public class ConstantDeclarationTypeAdapterFactory implements TypeAdapterFactory {

  @Nullable
  @Override
  @SuppressWarnings("unchecked")
  public <T> TypeAdapter<T> create(@Nullable final Gson gson, @Nullable final TypeToken<T> type) {
    if (type == null || ConstantDeclaration.class != type.getRawType()) {
      return null;
    }
    return (TypeAdapter<T>) new ConstantDeclarationTypeAdapter();
  }

}
