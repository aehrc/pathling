package au.csiro.pathling.fhirpath.function;

import static java.util.Objects.isNull;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.Concepts;
import au.csiro.pathling.fhirpath.EvaluationContext;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.TypeSpecifier;
import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.EmptyCollection;
import au.csiro.pathling.fhirpath.path.ParserPaths;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import lombok.Value;

@Value
public class FunctionParameterResolver {

  /**
   * The evaluation context for resolving variables and executing expressions
   */
  EvaluationContext evaluationContext;

  /**
   * The input collection that the function will operate on
   */
  Collection input;

  /**
   * The list of FHIRPath expressions that will be bound to function parameters
   */
  List<FhirPath> actualArguments;


  /**
   * Represents a function method with its bound arguments, ready for invocation.
   */
  @Value(staticConstructor = "of")
  public static class FunctionInvocation {

    /**
     * The method to be invoked
     */
    Method method;

    /**
     * The resolved arguments to pass to the method
     */
    Object[] arguments;

    /**
     * Invokes the function with the bound arguments.
     *
     * @return The result of the function invocation as a Collection
     * @throws RuntimeException if the invocation fails
     */
    public Collection invoke() {
      try {
        return (Collection) method.invoke(null, arguments);
      } catch (final IllegalAccessException | InvocationTargetException e) {
        throw new RuntimeException("Error invoking function: " + method.getName(), e);
      }
    }
  }

  /**
   * Provides context for parameter binding operations, including error reporting.
   * <p>
   * This class helps generate consistent, informative error messages that include the function name
   * and context (input or specific argument).
   */
  @Value
  public static class BindingContext {

    /**
     * The method being bound
     */
    @Nonnull
    Method method;

    /**
     * Description of the current binding context (e.g., "input" or "argument 0")
     */
    @Nullable
    String contextDescription;

    /**
     * Creates a binding context for a method.
     *
     * @param method The method being bound
     * @return A new binding context
     */
    @Nonnull
    public static BindingContext forMethod(@Nonnull final Method method) {
      return new BindingContext(method, null);
    }

    /**
     * Creates a binding context for the input of a method.
     *
     * @return A new binding context for the input
     */
    @Nonnull
    public BindingContext forInput() {
      return new BindingContext(method, "input");
    }

    /**
     * Creates a binding context for an argument of a method.
     *
     * @param index The index of the argument
     * @param parameterType The type of the parameter
     * @return A new binding context for the argument
     */
    @Nonnull
    public BindingContext forArgument(final int index, @Nonnull final Class<?> parameterType) {
      return new BindingContext(method,
          "argument " + index + " (" + parameterType.getSimpleName() + ")");
    }

    /**
     * Reports an error in this binding context.
     *
     * @param issue The issue to report
     * @return Never returns, always throws an exception
     * @throws InvalidUserInputError Always thrown with a formatted message
     */
    public <T> T reportError(@Nonnull final String issue) {
      final StringBuilder message = new StringBuilder("Function '")
          .append(method.getName())
          .append("'");

      if (contextDescription != null) {
        message.append(", ").append(contextDescription);
      }

      message.append(": ").append(issue);
      throw new InvalidUserInputError(message.toString());
    }

    /**
     * Checks a condition and reports an error if it's false.
     *
     * @param condition The condition to check
     * @param issue The issue to report if the condition is false
     * @throws InvalidUserInputError Thrown with a formatted message if condition is false
     */
    public void check(final boolean condition, @Nonnull final String issue) {
      if (!condition) {
        reportError(issue);
      }
    }
  }

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
        functionInput.getContext(),
        functionInput.getInput(),
        functionInput.getArguments());
  }


  /**
   * Binds the input and arguments to the specified method.
   * <p>
   * This method performs type checking and conversion of FHIRPath expressions to Java objects that
   * can be passed to the method. It validates that:
   * <ul>
   *   <li>The method has at least one parameter (for the input)</li>
   *   <li>The number of arguments does not exceed the method's parameter count</li>
   *   <li>Required parameters have corresponding arguments</li>
   *   <li>Arguments can be converted to the expected parameter types</li>
   * </ul>
   *
   * @param method The method to bind parameters to
   * @return A FunctionInvocation object ready to be executed
   * @throws RuntimeException if the method has no parameters
   * @throws InvalidUserInputError if the arguments cannot be bound to the parameters
   */
  @Nonnull
  public FunctionInvocation bind(@Nonnull final Method method) {

    // this is the  actual runtime error as this is an unexpected situation
    if (method.getParameterCount() == 0) {
      throw new RuntimeException("Function '" + method.getName()
          + "' does not accept any parameters and is a not a valid Fhirpath function backend.");
    }

    final BindingContext context = BindingContext.forMethod(method);

    // Resolve the input.
    final Object resolvedInput = resolveCollection(input, method.getParameters()[0],
        context.forInput());

    // Check that not extra arguments are provided.
    context.check(actualArguments.size() <= method.getParameterCount() - 1,
        "Too many arguments provided. Expected "
            + (method.getParameterCount() - 1) + ", got " + actualArguments.size());

    // Resolve each pair of method parameter and argument.
    final Stream<Object> resolvedArguments = IntStream.range(0, method.getParameterCount() - 1)
        .mapToObj(i -> {
          final Parameter parameter = method.getParameters()[i + 1];
          return resolveArgument(parameter,
              (i < actualArguments.size())
              ? actualArguments.get(i)
              : null,
              context.forArgument(i, parameter.getType()));
        });

    return FunctionInvocation.of(method,
        Stream.concat(Stream.of(resolvedInput), resolvedArguments)
            .toArray(Object[]::new));
  }

  /**
   * Resolves a FHIRPath argument to a Java object that can be passed to a method parameter.
   * <p>
   * This method handles different parameter types:
   * <ul>
   *   <li>Collection and Concepts - evaluates the FHIRPath and converts to the appropriate type</li>
   *   <li>CollectionTransform - creates a transform that applies the FHIRPath with the current context</li>
   *   <li>TypeSpecifier - extracts the TypeSpecifier value from a TypeSpecifierPath</li>
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
  private Object resolveArgument(@Nonnull final Parameter parameter,
      @Nullable final FhirPath argument, @Nonnull final BindingContext context) {

    if (isNull(argument)) {
      // check the parameter is happy with a null value
      if (parameter.getAnnotation(Nullable.class) != null) {
        return null;
      } else {
        return context.reportError("Parameter is required but no argument was provided");
      }
    } else if (Collection.class.isAssignableFrom(parameter.getType())
        || Concepts.class.isAssignableFrom(parameter.getType())) {
      // evaluate collection types 
      return resolveCollection(
          argument.apply(evaluationContext.getInputContext(), evaluationContext),
          parameter, context);
    } else if (CollectionTransform.class.isAssignableFrom(parameter.getType())) {
      // bind with context
      return (CollectionTransform) (c -> argument.apply(c, evaluationContext));
    } else if (TypeSpecifier.class.isAssignableFrom(parameter.getType())) {
      // bind type specifier
      if (argument instanceof ParserPaths.TypeSpecifierPath typeSpecifierPath) {
        return typeSpecifierPath.getValue();
      } else {
        return context.reportError(
            "Expected a type specifier but got " + argument.getClass().getSimpleName());
      }
    } else {
      // This is an unexpected situation - likely a programming error
      throw new RuntimeException("Cannot resolve parameter type: " + parameter.getType().getName());
    }
  }

  /**
   * Resolves a Collection to a type that can be passed to a method parameter.
   * <p>
   * This method handles:
   * <ul>
   *   <li>Converting collections to BooleanCollection when the parameter type is BooleanCollection</li>
   *   <li>Converting collections to Concepts when the parameter type is Concepts</li>
   *   <li>Converting EmptyCollection to specific collection types when needed</li>
   *   <li>Passing collections directly when the parameter type is assignable from the collection type</li>
   * </ul>
   *
   * @param collection The collection to resolve
   * @param parameter The method parameter to resolve the collection for
   * @param context The binding context for error reporting
   * @return The resolved collection value
   * @throws InvalidUserInputError if the collection cannot be converted to the parameter type
   */
  @Nonnull
  private Object resolveCollection(@Nonnull final Collection collection,
      @Nonnull final Parameter parameter, @Nonnull final BindingContext context) {
    if (BooleanCollection.class.isAssignableFrom(parameter.getType())) {
      return collection.asBooleanPath();
    } else if (Concepts.class.isAssignableFrom(parameter.getType())) {
      // evaluate collection types 
      return collection.toConcepts().orElseGet(
          () -> context.reportError("Cannot convert collection of type " +
              collection.getClass().getSimpleName() + " to Concepts")
      );
    } else if (parameter.getType().isAssignableFrom(collection.getClass())) {
      // evaluate collection types 
      return collection;
    } else if (collection instanceof EmptyCollection && Collection.class.isAssignableFrom(
        parameter.getType())) {
      // Handle empty collection conversion to specific collection types.
      return convertEmptyCollectionToType(parameter.getType(), context);
    } else {
      return context.reportError("Type mismatch: expected " + parameter.getType().getSimpleName() +
          " but got " + collection.getClass().getSimpleName());
    }
  }

  /**
   * Converts an EmptyCollection to a specific collection type by calling the static empty()
   * method.
   * <p>
   * This method uses reflection to invoke the static empty() method on the target collection class.
   * According to the FHIRPath specification, empty collections should be handled gracefully by
   * functions, and functions that operate on empty collections should return appropriate empty
   * results.
   *
   * @param targetType The target collection class
   * @param context The binding context for error reporting
   * @return An empty instance of the target collection type
   * @throws InvalidUserInputError if the conversion cannot be performed
   */
  @Nonnull
  private Object convertEmptyCollectionToType(@Nonnull final Class<?> targetType,
      @Nonnull final BindingContext context) {
    try {
      // Try to find and invoke the static empty() method on the target collection class
      final Method emptyMethod = targetType.getMethod("empty");
      return emptyMethod.invoke(null);
    } catch (final NoSuchMethodException e) {
      return context.reportError(
          "Cannot convert empty collection to " + targetType.getSimpleName() +
              ": no static empty() method found");
    } catch (final IllegalAccessException | InvocationTargetException e) {
      return context.reportError(
          "Cannot convert empty collection to " + targetType.getSimpleName() +
              ": failed to invoke empty() method - " + e.getMessage());
    }
  }
}
