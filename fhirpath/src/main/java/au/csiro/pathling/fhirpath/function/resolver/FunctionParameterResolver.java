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

package au.csiro.pathling.fhirpath.function.resolver;

import static java.util.Objects.isNull;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.EvaluationContext;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.TerminologyConcepts;
import au.csiro.pathling.fhirpath.TypeSpecifier;
import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.EmptyCollection;
import au.csiro.pathling.fhirpath.function.CollectionTransform;
import au.csiro.pathling.fhirpath.function.FunctionInput;
import au.csiro.pathling.fhirpath.path.ParserPaths;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Resolves function parameters for method-defined FHIRPath functions.
 *
 * @param evaluationContext The evaluation context for resolving variables and executing expressions
 * @param input The input collection that the function will operate on
 * @param actualArguments The list of FHIRPath expressions that will be bound to function parameters
 * @author Piotr Szul
 * @author John Grimes
 */
public record FunctionParameterResolver(
    EvaluationContext evaluationContext, Collection input, List<FhirPath> actualArguments) {

  /**
   * Creates a FunctionParameterResolver from a FunctionInput object.
   *
   * @param functionInput The function input containing context, input collection, and arguments
   * @return A new FunctionParameterResolver
   */
  @Nonnull
  public static FunctionParameterResolver fromFunctionInput(
      @Nonnull final FunctionInput functionInput) {
    return new FunctionParameterResolver(
        functionInput.context(), functionInput.input(), functionInput.arguments());
  }

  /**
   * Binds the input and arguments to the specified method.
   *
   * <p>This method performs type checking and conversion of FHIRPath expressions to Java objects
   * that can be passed to the method. It validates that:
   *
   * <ul>
   *   <li>The method has at least one parameter (for the input)
   *   <li>The number of arguments does not exceed the method's parameter count
   *   <li>Required parameters have corresponding arguments
   *   <li>Arguments can be converted to the expected parameter types
   * </ul>
   *
   * @param method The method to bind parameters to
   * @return A FunctionInvocation object ready to be executed
   * @throws RuntimeException if the method has no parameters
   * @throws InvalidUserInputError if the arguments cannot be bound to the parameters
   */
  @Nonnull
  public FunctionInvocation bind(@Nonnull final Method method) {

    // This check ensures that the method has at least one parameter.
    if (method.getParameterCount() == 0) {
      throw new AssertionError(
          "Function '"
              + method.getName()
              + "' does not accept any parameters and is a not a valid FhirPath function"
              + " implementation");
    }

    final BindingContext context = BindingContext.forMethod(method);

    // Resolve the input.
    final Object resolvedInput =
        resolveCollection(input, method.getParameters()[0], context.forInput());

    // Check that not extra arguments are provided.
    context.check(
        actualArguments.size() <= method.getParameterCount() - 1,
        "Too many arguments provided. Expected "
            + (method.getParameterCount() - 1)
            + ", got "
            + actualArguments.size());

    // Resolve each pair of method parameter and argument.
    final Stream<Object> resolvedArguments =
        IntStream.range(0, method.getParameterCount() - 1)
            .mapToObj(
                i -> {
                  final Parameter parameter = method.getParameters()[i + 1];
                  return resolveArgument(
                      parameter,
                      (i < actualArguments.size()) ? actualArguments.get(i) : null,
                      context.forArgument(i, parameter.getType()));
                });

    return new FunctionInvocation(
        method, Stream.concat(Stream.of(resolvedInput), resolvedArguments).toArray(Object[]::new));
  }

  /**
   * Resolves a FHIRPath argument to a Java object that can be passed to a method parameter.
   *
   * <p>This method handles different parameter types:
   *
   * <ul>
   *   <li>Collection and TerminologyConcepts - evaluates the FHIRPath and converts to the
   *       appropriate type
   *   <li>CollectionTransform - creates a transform that applies the FHIRPath with the current
   *       context
   *   <li>TypeSpecifier - extracts the TypeSpecifier value from a TypeSpecifierPath
   * </ul>
   *
   * @param parameter The method parameter to resolve an argument for
   * @param argument The FHIRPath argument to resolve, may be null
   * @param context The binding context for error reporting
   * @return The resolved argument value, or null for optional parameters
   * @throws InvalidUserInputError if the argument cannot be resolved to the parameter type
   * @throws RuntimeException if the parameter type is not supported
   */
  @Nullable
  private Object resolveArgument(
      @Nonnull final Parameter parameter,
      @Nullable final FhirPath argument,
      @Nonnull final BindingContext context) {

    if (isNull(argument)) {
      // check the parameter is happy with a null value
      if (parameter.getAnnotation(Nullable.class) != null) {
        return null;
      } else {
        return context.reportError("Parameter is required but no argument was provided");
      }
    } else if (Collection.class.isAssignableFrom(parameter.getType())
        || TerminologyConcepts.class.isAssignableFrom(parameter.getType())) {
      // evaluate collection types
      return resolveCollection(
          argument.apply(evaluationContext.getInputContext(), evaluationContext),
          parameter,
          context);
    } else if (CollectionTransform.class.isAssignableFrom(parameter.getType())) {
      // bind with context
      return (CollectionTransform) (c -> argument.apply(c, evaluationContext));
    } else if (TypeSpecifier.class.isAssignableFrom(parameter.getType())) {
      // bind type specifier
      if (argument instanceof final ParserPaths.TypeSpecifierPath typeSpecifierPath) {
        return typeSpecifierPath.getValue();
      } else {
        return context.reportError(
            "Expected a type specifier but got " + argument.getClass().getSimpleName());
      }
    } else {
      // This is an unexpected situation - likely a programming error
      throw new AssertionError("Cannot resolve parameter type: " + parameter.getType().getName());
    }
  }

  /**
   * Resolves a Collection to a type that can be passed to a method parameter.
   *
   * <p>This method handles:
   *
   * <ul>
   *   <li>Converting collections to BooleanCollection when the parameter type is BooleanCollection
   *   <li>Converting collections to TerminologyConcepts when the parameter type is
   *       TerminologyConcepts
   *   <li>Converting EmptyCollection to specific collection types when needed
   *   <li>Passing collections directly when the parameter type is assignable from the collection
   *       type
   * </ul>
   *
   * @param collection The collection to resolve
   * @param parameter The method parameter to resolve the collection for
   * @param context The binding context for error reporting
   * @return The resolved collection value
   * @throws InvalidUserInputError if the collection cannot be converted to the parameter type
   */
  @Nonnull
  private Object resolveCollection(
      @Nonnull final Collection collection,
      @Nonnull final Parameter parameter,
      @Nonnull final BindingContext context) {
    // Check if the parameter expects a BooleanCollection type, if so convert the collection to a
    // Boolean representation.
    if (BooleanCollection.class.isAssignableFrom(parameter.getType())) {
      return collection.asBooleanPath();

    } else if (TerminologyConcepts.class.isAssignableFrom(parameter.getType())) {
      // Check if the parameter expects a TerminologyConcepts type. Attempt to convert the
      // collection to TerminologyConcepts, reporting an error if conversion fails.
      return collection
          .toConcepts()
          .orElseGet(
              () ->
                  context.reportError(
                      "Cannot convert collection of type "
                          + collection.getClass().getSimpleName()
                          + " to TerminologyConcepts"));

    } else if (parameter.getType().isAssignableFrom(collection.getClass())) {
      // Check if the parameter type can directly accept the collection type. Return the collection
      // directly as it's already compatible.
      return collection;

    } else if (collection instanceof EmptyCollection
        && Collection.class.isAssignableFrom(parameter.getType())) {
      // Handle the case where we have an empty collection and the parameter expects a Collection
      // type. Convert the empty collection to the expected collection type.
      return convertEmptyCollectionToType(parameter.getType(), context);

    } else {
      // None of the above conditions matched, indicating a type mismatch. Report an error for
      // unsupported type conversion.
      return context.reportError(
          "Type mismatch: expected "
              + parameter.getType().getSimpleName()
              + " but got "
              + collection.getClass().getSimpleName());
    }
  }

  /**
   * Converts an EmptyCollection to a specific collection type by calling the static empty() method.
   *
   * <p>This method uses reflection to invoke the static empty() method on the target collection
   * class. According to the FHIRPath specification, empty collections should be handled gracefully
   * by functions, and functions that operate on empty collections should return appropriate empty
   * results.
   *
   * @param targetType The target collection class
   * @param context The binding context for error reporting
   * @return An empty instance of the target collection type
   * @throws InvalidUserInputError if the conversion cannot be performed
   */
  @Nonnull
  private Object convertEmptyCollectionToType(
      @Nonnull final Class<?> targetType, @Nonnull final BindingContext context) {
    try {
      // Try to find and invoke the static empty() method on the target collection class.
      final Method emptyMethod = targetType.getMethod("empty");
      return emptyMethod.invoke(null);
    } catch (final NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      return context.reportError(
          "Cannot convert empty collection to " + targetType.getSimpleName());
    }
  }
}
