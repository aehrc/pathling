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
import jakarta.annotation.Nonnull;
import java.util.Collection;
import java.util.Optional;
import java.util.function.Function;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.hl7.fhir.r4.model.Type;

/**
 * Utility methods for extracting parameters from FHIR Parameters resources.
 *
 * @author Felix Naumann
 * @author John Grimes
 */
public final class ParamUtil {

  private ParamUtil() {
    // Utility class.
  }

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
  @Nonnull
  public static <T> Optional<Collection<T>> extractManyFromParameters(
      @Nonnull final Collection<ParametersParameterComponent> parts,
      @Nonnull final String partName,
      @Nonnull final Class<T> typeClazz,
      final boolean lenient,
      @Nonnull final Optional<RuntimeException> onError) {
    return extractManyFromParameters(
        parts, partName, typeClazz, false, Optional.empty(), lenient, onError);
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
  @Nonnull
  public static <T extends Type> Optional<Collection<T>> extractManyFromParameters(
      @Nonnull final Collection<ParametersParameterComponent> parts,
      @Nonnull final String partName,
      @Nonnull final Class<T> typeClazz,
      @Nonnull final Optional<Collection<T>> defaultValue,
      final boolean lenient) {
    return extractManyFromParameters(
        parts, partName, typeClazz, true, defaultValue, lenient, Optional.empty());
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
  @Nonnull
  public static <T> Optional<Collection<T>> extractManyFromParameters(
      @Nonnull final Collection<ParametersParameterComponent> parts,
      @Nonnull final String partName,
      @Nonnull final Class<T> typeClazz,
      final boolean useDefaultValueOnEmpty,
      @Nonnull final Optional<Collection<T>> defaultValue,
      final boolean lenient,
      @Nonnull final Optional<RuntimeException> onError) {
    final Collection<T> types =
        parts.stream()
            .filter(param -> partName.equals(param.getName()))
            .map(typeClazz::cast)
            .toList();
    if (!types.isEmpty()) {
      return Optional.of(types);
    }
    if (useDefaultValueOnEmpty || lenient) {
      return defaultValue;
    }
    throw onError.orElseThrow(
        () ->
            new InvalidUserInputError(
                "Required parameter '%s' is missing and no error handler provided"
                    .formatted(partName)));
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
  @Nonnull
  public static <T, R> Optional<R> extractFromPart(
      @Nonnull final Collection<ParametersParameterComponent> parts,
      @Nonnull final String partName,
      @Nonnull final Class<? extends T> clazz,
      @Nonnull final Function<T, R> mapper,
      final boolean lenient,
      @Nonnull final Optional<RuntimeException> onError) {
    return extractFromPart(
        parts, partName, clazz, mapper, false, Optional.empty(), lenient, onError);
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
  @Nonnull
  public static <T, R> Optional<R> extractFromPart(
      @Nonnull final Collection<ParametersParameterComponent> parts,
      @Nonnull final String partName,
      @Nonnull final Class<? extends T> typeClazz,
      @Nonnull final Function<T, R> mapper,
      final boolean useDefaultValue,
      @Nonnull final Optional<R> defaultValue,
      final boolean lenient) {
    return extractFromPart(
        parts,
        partName,
        typeClazz,
        mapper,
        useDefaultValue,
        defaultValue,
        lenient,
        Optional.empty());
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
  @Nonnull
  public static <T, R> Optional<R> extractFromPart(
      @Nonnull final Collection<ParametersParameterComponent> parts,
      @Nonnull final String partName,
      @Nonnull final Class<? extends T> typeClazz,
      @Nonnull final Function<T, R> mapper,
      final boolean useDefaultValue,
      @Nonnull final Optional<R> defaultValue,
      final boolean lenient,
      @Nonnull final Optional<RuntimeException> onError) {
    final Optional<Type> type =
        parts.stream()
            .filter(param -> partName.equals(param.getName()))
            .findFirst()
            .map(ParametersParameterComponent::getValue);

    if (type.isEmpty()) {
      return handleMissingParameter(partName, useDefaultValue, lenient, defaultValue, onError);
    }

    final T casted = castParameterValue(type.get(), partName, typeClazz, onError);
    return applyMapperWithFallback(casted, mapper, useDefaultValue, lenient, defaultValue, onError);
  }

  /**
   * Casts a parameter value to the expected type.
   *
   * @param value the value to cast
   * @param partName the parameter name for error messages
   * @param typeClazz the expected type class
   * @param onError custom exception to throw on error
   * @param <T> the target type
   * @return the cast value
   * @throws InvalidUserInputError if the value cannot be cast
   */
  @Nonnull
  private static <T> T castParameterValue(
      @Nonnull final Type value,
      @Nonnull final String partName,
      @Nonnull final Class<? extends T> typeClazz,
      @Nonnull final Optional<RuntimeException> onError) {
    try {
      return typeClazz.cast(value);
    } catch (final ClassCastException e) {
      if (onError.isPresent()) {
        onError.get().initCause(e);
        throw onError.get();
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
   * @param value the value to map
   * @param mapper the mapping function
   * @param useDefaultValue whether to use default on error
   * @param lenient whether to be lenient on errors
   * @param defaultValue the default value to return on error
   * @param onError custom exception to throw on error
   * @param <T> the input type
   * @param <R> the result type
   * @return the mapped result or default value wrapped in Optional
   */
  @Nonnull
  private static <T, R> Optional<R> applyMapperWithFallback(
      @Nonnull final T value,
      @Nonnull final Function<T, R> mapper,
      final boolean useDefaultValue,
      final boolean lenient,
      @Nonnull final Optional<R> defaultValue,
      @Nonnull final Optional<RuntimeException> onError) {
    try {
      return Optional.of(mapper.apply(value));
    } catch (final IllegalArgumentException e) {
      if (lenient && useDefaultValue) {
        return defaultValue;
      }
      if (onError.isPresent()) {
        onError.get().initCause(e);
        throw onError.get();
      }
      throw e;
    }
  }

  /**
   * Handles the case when a parameter is missing.
   *
   * @param partName the name of the missing parameter
   * @param useDefaultValue whether to return the default value
   * @param lenient whether to be lenient and return empty
   * @param defaultValue the default value to return
   * @param onError the exception to throw if not lenient
   * @param <R> the result type
   * @return the default value wrapped in Optional, or empty if lenient
   * @throws RuntimeException if parameter is required
   */
  @Nonnull
  private static <R> Optional<R> handleMissingParameter(
      @Nonnull final String partName,
      final boolean useDefaultValue,
      final boolean lenient,
      @Nonnull final Optional<R> defaultValue,
      @Nonnull final Optional<RuntimeException> onError) {
    if (useDefaultValue) {
      return defaultValue;
    }
    if (lenient) {
      return Optional.empty();
    }
    throw onError.orElseThrow(
        () ->
            new InvalidUserInputError(
                "Required parameter '%s' is missing and no error handler provided"
                    .formatted(partName)));
  }
}
