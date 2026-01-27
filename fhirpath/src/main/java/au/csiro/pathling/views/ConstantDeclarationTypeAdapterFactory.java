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
