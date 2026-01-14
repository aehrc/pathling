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

package au.csiro.pathling;

import au.csiro.pathling.errors.InvalidUserInputError;
import jakarta.annotation.Nullable;
import java.util.Collection;
import java.util.Optional;
import java.util.function.Function;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.hl7.fhir.r4.model.Type;

/**
 * @author Felix Naumann
 */
public class ParamUtil {

  public static <T> Collection<T> extractManyFromParameters(
      Collection<ParametersParameterComponent> parts,
      String partName,
      Class<T> typeClazz,
      boolean lenient,
      RuntimeException onError) {
    return extractManyFromParameters(parts, partName, typeClazz, false, null, lenient, onError);
  }

  public static <T extends Type> Collection<T> extractManyFromParameters(
      Collection<ParametersParameterComponent> parts,
      String partName,
      Class<T> typeClazz,
      @Nullable Collection<T> defaultValue,
      boolean lenient) {
    return extractManyFromParameters(parts, partName, typeClazz, true, defaultValue, lenient, null);
  }

  public static <T> Collection<T> extractManyFromParameters(
      Collection<ParametersParameterComponent> parts,
      String partName,
      Class<T> typeClazz,
      boolean useDefaultValueOnEmpty,
      @Nullable Collection<T> defaultValue,
      boolean lenient,
      RuntimeException onError) {
    Collection<T> types =
        parts.stream()
            .filter(param -> partName.equals(param.getName()))
            .map(typeClazz::cast)
            .toList();
    if (!types.isEmpty()) {
      return types;
    }
    if (useDefaultValueOnEmpty || lenient) {
      return defaultValue;
    }
    throw onError;
  }

  public static <T, R> R extractFromPart(
      Collection<ParametersParameterComponent> parts,
      String partName,
      Class<? extends T> clazz,
      Function<T, R> mapper,
      boolean lenient,
      RuntimeException onError) {
    return extractFromPart(parts, partName, clazz, mapper, false, null, lenient, onError);
  }

  public static <T, R> R extractFromPart(
      Collection<ParametersParameterComponent> parts,
      String partName,
      Class<? extends T> typeClazz,
      Function<T, R> mapper,
      boolean useDefaultValue,
      @Nullable R defaultValue,
      boolean lenient) {
    return extractFromPart(
        parts, partName, typeClazz, mapper, useDefaultValue, defaultValue, lenient, null);
  }

  @SuppressWarnings("java:S3776") // Complexity is inherent to flexible parameter extraction.
  public static <T, R> R extractFromPart(
      Collection<ParametersParameterComponent> parts,
      String partName,
      Class<? extends T> typeClazz,
      Function<T, R> mapper,
      boolean useDefaultValue,
      @Nullable R defaultValue,
      boolean lenient,
      RuntimeException onError) {
    Optional<Type> type =
        parts.stream()
            .filter(param -> partName.equals(param.getName()))
            .findFirst()
            .map(ParametersParameterComponent::getValue);
    if (type.isPresent()) {
      final T casted;
      try {
        casted = typeClazz.cast(type.get());
      } catch (final ClassCastException e) {
        // Convert ClassCastException to InvalidUserInputError for proper HTTP 400 response.
        if (onError != null) {
          onError.initCause(e);
          throw onError;
        }
        throw new InvalidUserInputError(
            "Invalid parameter type for '%s': expected %s but got %s"
                .formatted(
                    partName, typeClazz.getSimpleName(), type.get().getClass().getSimpleName()),
            e);
      }
      try {
        return mapper.apply(casted);
      } catch (final IllegalArgumentException e) {
        if (lenient && useDefaultValue) {
          return defaultValue;
        }
        if (onError != null) {
          onError.initCause(e);
          throw onError;
        }
        throw e;
      }
    }
    if (useDefaultValue || lenient) {
      return defaultValue;
    }
    throw onError;
  }
}
