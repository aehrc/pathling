package au.csiro.pathling.fhirpath.function;

import static java.util.Objects.isNull;

import au.csiro.pathling.fhirpath.EvaluationContext;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.TypeSpecifier;
import au.csiro.pathling.fhirpath.collection.CodingCollection;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.path.Paths;
import java.lang.reflect.Parameter;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Value;

@Value
class FunctionParameterResolver {

  EvaluationContext evaluationContext;
  Collection input;

  @Nullable
  public Object resolveArgument(@Nonnull final Parameter parameter,
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
    } else if (Collection.class.isAssignableFrom(parameter.getType())) {
      // evaluate collection types 
      return resolveCollection(argument.apply(input, evaluationContext),
          parameter);
    } else if (CollectionTransform.class.isAssignableFrom(parameter.getType())) {
      // bind with context
      return (CollectionTransform) (c -> argument.apply(c, evaluationContext));
    } else if (TypeSpecifier.class.isAssignableFrom(parameter.getType())) {
      // bind type specifier
      return ((Paths.TypeSpecifierPath) argument).getTypeSpecifier();
    } else if (FhirPath.class.isAssignableFrom(parameter.getType())) {
      // bind type specifier
      return argument;
    } else {
      throw new RuntimeException("Cannot resolve parameter:" + parameter);
    }
  }

  public Object resolveCollection(@Nonnull final Collection collection,
      @Nonnull final Parameter parameter) {
    if (CodingCollection.class.isAssignableFrom(parameter.getType())) {
      // evaluate collection types 
      return collection.asCoding().orElseThrow();
    } else if (Collection.class.isAssignableFrom(parameter.getType())) {
      // evaluate collection types 
      return collection;
    } else {
      throw new RuntimeException("Cannot resolve input:" + parameter);
    }
  }
 
}
