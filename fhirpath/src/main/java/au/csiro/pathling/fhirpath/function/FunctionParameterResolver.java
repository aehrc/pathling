package au.csiro.pathling.fhirpath.function;

import static au.csiro.pathling.utilities.Preconditions.checkUserInput;
import static java.util.Objects.isNull;

import au.csiro.pathling.fhirpath.Concepts;
import au.csiro.pathling.fhirpath.EvaluationContext;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.TypeSpecifier;
import au.csiro.pathling.fhirpath.collection.Collection;
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

  EvaluationContext evaluationContext;
  Collection input;
  List<FhirPath> actualArguments;


  @Value(staticConstructor = "of")
  public static class FunctionInvocation {

    Method method;
    Object[] arguments;

    public Collection invoke() {
      try {
        return (Collection) method.invoke(null, arguments);
      } catch (final IllegalAccessException | InvocationTargetException e) {
        throw new RuntimeException("Error invoking function: " + method.getName(), e);
      }
    }

  }

  @Nonnull
  public static FunctionParameterResolver fromFunctionInput(
      @Nonnull final FunctionInput functionInput) {
    return new FunctionParameterResolver(
        functionInput.getContext(),
        functionInput.getInput(),
        functionInput.getArguments());
  }


  @Nonnull
  public FunctionInvocation bind(@Nonnull final Method method) {
    // Check that not extra arguments are provided.
    checkUserInput(actualArguments.size() <= method.getParameterCount() - 1,
        "Too many arguments provided for function '" + method.getName() + "'. Expected "
            + (method.getParameterCount() - 1) + ", got " + actualArguments.size());

    // Resolve each pair of method parameter and argument.
    final Stream<Object> resolvedArguments = IntStream.range(0, method.getParameterCount() - 1)
        .mapToObj(i -> resolveArgument(method.getParameters()[i + 1],
            (i < actualArguments.size())
            ? actualArguments.get(i)
            : null));

    // Resolve the input.
    final Object resolvedInput = resolveCollection(input, method.getParameters()[0]);
    return FunctionInvocation.of(method,
        Stream.concat(Stream.of(resolvedInput), resolvedArguments)
            .toArray(Object[]::new));
  }


  @Nullable
  Object resolveArgument(@Nonnull final Parameter parameter,
      final FhirPath argument) {

    if (isNull(argument)) {
      // check the pararmeter is happy with a null value
      if (EvaluationContext.class.isAssignableFrom(parameter.getType())) {
        // bind type specifier
        return evaluationContext;
      } else if (parameter.getAnnotation(Nullable.class) != null) {
        return null;
      } else {
        throw new RuntimeException(
            "Parameter " + parameter + " is not nullable and no argument was provided");
      }
      // return Optional.ofNullable(parameter.getAnnotation(Nullable.class))
      //     .map(__ -> null).orElseThrow(() -> new RuntimeException(
      //         "Parameter " + parameter + " is not nullable and no argument was provided"));
    } else if (Collection.class.isAssignableFrom(parameter.getType())
        || Concepts.class.isAssignableFrom(parameter.getType())) {
      // evaluate collection types 
      return resolveCollection(
          argument.apply(evaluationContext.getInputContext(),
              evaluationContext),
          parameter);
    } else if (CollectionTransform.class.isAssignableFrom(parameter.getType())) {
      // bind with context
      return (CollectionTransform) (c -> argument.apply(c, evaluationContext));
    } else if (TypeSpecifier.class.isAssignableFrom(parameter.getType())) {
      // bind type specifier
      return ((ParserPaths.TypeSpecifierPath) argument).getValue();


    } else if (FhirPath.class.isAssignableFrom(parameter.getType())) {
      // bind type specifier
      return argument;
    } else {
      throw new RuntimeException("Cannot resolve parameter:" + parameter);
    }
  }

  Object resolveCollection(@Nonnull final Collection collection,
      @Nonnull final Parameter parameter) {
    if (Concepts.class.isAssignableFrom(parameter.getType())) {
      // evaluate collection types 
      return collection.toConcepts().orElseThrow(
          () -> new IllegalArgumentException("Cannot convert collection to concepts")
      );
    } else if (parameter.getType().isAssignableFrom(collection.getClass())) {
      // evaluate collection types 
      return collection;
    } else {
      throw new RuntimeException(
          "Cannot resolve input, expected:" + parameter.getType() + " and got:"
              + collection.getClass());
    }
  }

}
