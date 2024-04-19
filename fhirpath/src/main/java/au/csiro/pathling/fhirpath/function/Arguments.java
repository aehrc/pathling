/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.fhirpath.function;

import static java.util.Objects.requireNonNull;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.literal.LiteralPath;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Optional;
import org.hl7.fhir.r4.model.Type;

/**
 * Helper class for dealing with optional arguments.
 */
public class Arguments {

  @Nonnull
  private final List<FhirPath> arguments;

  private Arguments(@Nonnull final List<FhirPath> arguments) {
    this.arguments = arguments;
  }

  /**
   * Gets the value of an optional literal argument or the default value it the argument is
   * missing.
   *
   * @param index the 0-based index of the argument
   * @param defaultValue the default value
   * @param <T> the Java type of the argument value
   * @return the java value of the requested argument
   */
  @SuppressWarnings("unchecked")
  @Nonnull
  public <T extends Type> T getValueOr(final int index, @Nonnull final T defaultValue) {
    return (index < arguments.size())
           ? getValue(index, (Class<T>) defaultValue.getClass())
           : defaultValue;
  }

  /**
   * Gets the value of an optional literal argument that does not have a default value.
   *
   * @param index the 0-based index of the argument
   * @param valueClass the expected Java class of the argument value
   * @param <T> the Java type of the argument value
   * @return the java value of the requested argument
   */
  @Nullable
  public <T extends Type> T getNullableValue(final int index,
      @Nonnull final Class<T> valueClass) {
    return index < arguments.size()
           ? valueClass.cast(((LiteralPath<?>) arguments.get(index)).getValue())
           : null;
  }

  /**
   * Gets the value of an optional literal argument that does not have a default value.
   *
   * @param index the 0-based index of the argument
   * @param valueClass the expected Java class of the argument value
   * @param <T> the Java type of the argument value
   * @return an {@link Optional} containing the Java value of the requested argument, or an empty
   * {@link Optional} if the argument is missing
   */
  @Nonnull
  public <T extends Type> Optional<T> getOptionalValue(final int index,
      @Nonnull final Class<T> valueClass) {
    return Optional.ofNullable(getNullableValue(index, valueClass));
  }

  /**
   * Gets the value of the required literal argument.
   *
   * @param index the 0-based index of the argument
   * @param valueClass the expected Java class of the argument value
   * @param <T> the HAPI type of the argument value
   * @return the java value of the requested argument
   */
  @Nonnull
  public <T extends Type> T getValue(final int index, @Nonnull final Class<T> valueClass) {
    return requireNonNull(getNullableValue(index, valueClass));
  }

  /**
   * Construct Arguments for given {@link NamedFunctionInput}
   *
   * @param input the function input
   * @return the Arguments for the input
   */
  @Nonnull
  public static Arguments of(@Nonnull final NamedFunctionInput input) {
    return new Arguments(input.getArguments());
  }
}
