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
 * Utility methods for extracting parameters from FHIR Parameters resources.
 *
 * @author Felix Naumann
 */
public class ParamUtil {

  /**
   * Extracts multiple values of a specified type from a Parameters resource.
   *
   * @param parts the parameter parts to search
   * @param partName the name of the parameter to extract
   * @param typeClazz the expected type class
   * @param lenient whether to be lenient on missing parameters
   * @param onError the exception to throw if not lenient and parameter is missing
   * @param <T> the type of values to extract
   * @return the collection of extracted values
   */
  public static <T> Collection<T> extractManyFromParameters(
      final Collection<ParametersParameterComponent> parts,
      final String partName,
      final Class<T> typeClazz,
      final boolean lenient,
      final RuntimeException onError) {
    return extractManyFromParameters(parts, partName, typeClazz, false, null, lenient, onError);
  }

  /**
   * Extracts multiple values of a specified type from a Parameters resource with a default value.
   *
   * @param parts the parameter parts to search
   * @param partName the name of the parameter to extract
   * @param typeClazz the expected type class
   * @param defaultValue the default value if not found
   * @param lenient whether to be lenient on missing parameters
   * @param <T> the type of values to extract
   * @return the collection of extracted values, or default if not found
   */
  public static <T extends Type> Collection<T> extractManyFromParameters(
      final Collection<ParametersParameterComponent> parts,
      final String partName,
      final Class<T> typeClazz,
      @Nullable final Collection<T> defaultValue,
      final boolean lenient) {
    return extractManyFromParameters(parts, partName, typeClazz, true, defaultValue, lenient, null);
  }

  /**
   * Extracts multiple values of a specified type from a Parameters resource with full options.
   *
   * @param parts the parameter parts to search
   * @param partName the name of the parameter to extract
   * @param typeClazz the expected type class
   * @param useDefaultValueOnEmpty whether to use default when empty
   * @param defaultValue the default value if not found
   * @param lenient whether to be lenient on missing parameters
   * @param onError the exception to throw if not lenient and parameter is missing
   * @param <T> the type of values to extract
   * @return the collection of extracted values
   */
  public static <T> Collection<T> extractManyFromParameters(
      final Collection<ParametersParameterComponent> parts,
      final String partName,
      final Class<T> typeClazz,
      final boolean useDefaultValueOnEmpty,
      @Nullable final Collection<T> defaultValue,
      final boolean lenient,
      final RuntimeException onError) {
    final Collection<T> types =
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

  /**
   * Extracts a single value from a parameter part and maps it to a result type.
   *
   * @param parts the parameter parts to search
   * @param partName the name of the parameter to extract
   * @param clazz the expected type class
   * @param mapper the function to map the value to the result type
   * @param lenient whether to be lenient on missing parameters
   * @param onError the exception to throw if not lenient and parameter is missing
   * @param <T> the input type
   * @param <R> the result type
   * @return the mapped result
   */
  public static <T, R> R extractFromPart(
      final Collection<ParametersParameterComponent> parts,
      final String partName,
      final Class<? extends T> clazz,
      final Function<T, R> mapper,
      final boolean lenient,
      final RuntimeException onError) {
    return extractFromPart(parts, partName, clazz, mapper, false, null, lenient, onError);
  }

  /**
   * Extracts a single value from a parameter part with a default value.
   *
   * @param parts the parameter parts to search
   * @param partName the name of the parameter to extract
   * @param typeClazz the expected type class
   * @param mapper the function to map the value to the result type
   * @param useDefaultValue whether to use default when not found
   * @param defaultValue the default value if not found
   * @param lenient whether to be lenient on missing parameters
   * @param <T> the input type
   * @param <R> the result type
   * @return the mapped result, or default if not found
   */
  public static <T, R> R extractFromPart(
      final Collection<ParametersParameterComponent> parts,
      final String partName,
      final Class<? extends T> typeClazz,
      final Function<T, R> mapper,
      final boolean useDefaultValue,
      @Nullable final R defaultValue,
      final boolean lenient) {
    return extractFromPart(
        parts, partName, typeClazz, mapper, useDefaultValue, defaultValue, lenient, null);
  }

  /**
   * Extracts a single value from a parameter part with full options.
   *
   * @param parts the parameter parts to search
   * @param partName the name of the parameter to extract
   * @param typeClazz the expected type class
   * @param mapper the function to map the value to the result type
   * @param useDefaultValue whether to use default when not found
   * @param defaultValue the default value if not found
   * @param lenient whether to be lenient on missing parameters
   * @param onError the exception to throw if not lenient and parameter is missing
   * @param <T> the input type
   * @param <R> the result type
   * @return the mapped result
   */
  public static <T, R> R extractFromPart(
      Collection<ParametersParameterComponent> parts,
      String partName,
      Class<? extends T> typeClazz,
      Function<T, R> mapper,
      boolean useDefaultValue,
      @Nullable R defaultValue,
      boolean lenient,
      RuntimeException onError) {
    final Optional<Type> type =
        parts.stream()
            .filter(param -> partName.equals(param.getName()))
            .findFirst()
            .map(ParametersParameterComponent::getValue);

    if (type.isEmpty()) {
      return handleMissingParameter(useDefaultValue, lenient, defaultValue, onError);
    }

    final T casted = castParameterValue(type.get(), partName, typeClazz, onError);
    return applyMapperWithFallback(casted, mapper, useDefaultValue, lenient, defaultValue, onError);
  }

  /**
   * Casts a parameter value to the expected type.
   *
   * @param value The value to cast.
   * @param partName The parameter name for error messages.
   * @param typeClazz The expected type class.
   * @param onError Custom exception to throw on error (may be null).
   * @param <T> The target type.
   * @return The cast value.
   * @throws InvalidUserInputError If the value cannot be cast.
   */
  private static <T> T castParameterValue(
      @Nullable final Type value,
      final String partName,
      final Class<? extends T> typeClazz,
      @Nullable final RuntimeException onError) {
    try {
      return typeClazz.cast(value);
    } catch (final ClassCastException e) {
      if (onError != null) {
        onError.initCause(e);
        throw onError;
      }
      throw new InvalidUserInputError(
          "Invalid parameter type for '%s': expected %s but got %s"
              .formatted(partName, typeClazz.getSimpleName(), value.getClass().getSimpleName()),
          e);
    }
  }

  /**
   * Applies the mapper function with fallback handling for errors.
   *
   * @param value The value to map.
   * @param mapper The mapping function.
   * @param useDefaultValue Whether to use default on error.
   * @param lenient Whether to be lenient on errors.
   * @param defaultValue The default value to return on error.
   * @param onError Custom exception to throw on error (may be null).
   * @param <T> The input type.
   * @param <R> The result type.
   * @return The mapped result or default value.
   */
  private static <T, R> R applyMapperWithFallback(
      final T value,
      final Function<T, R> mapper,
      final boolean useDefaultValue,
      final boolean lenient,
      @Nullable final R defaultValue,
      @Nullable final RuntimeException onError) {
    try {
      return mapper.apply(value);
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

  /**
   * Handles the case when a parameter is missing.
   *
   * @param useDefaultValue Whether to return the default value.
   * @param lenient Whether to be lenient and return default.
   * @param defaultValue The default value to return.
   * @param onError The exception to throw if not lenient.
   * @param <R> The result type.
   * @return The default value if allowed.
   * @throws RuntimeException If parameter is required.
   */
  private static <R> R handleMissingParameter(
      final boolean useDefaultValue,
      final boolean lenient,
      @Nullable final R defaultValue,
      final RuntimeException onError) {
    if (useDefaultValue || lenient) {
      return defaultValue;
    }
    throw onError;
  }
}
