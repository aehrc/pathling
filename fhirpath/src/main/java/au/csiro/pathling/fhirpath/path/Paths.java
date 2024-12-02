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

package au.csiro.pathling.fhirpath.path;

import static au.csiro.pathling.utilities.Preconditions.checkUserInput;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.EvaluationContext;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.TypeSpecifier;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.StringCollection;
import au.csiro.pathling.fhirpath.function.FunctionInput;
import au.csiro.pathling.fhirpath.function.NamedFunction;
import au.csiro.pathling.fhirpath.function.registry.NoSuchFunctionException;
import au.csiro.pathling.fhirpath.operator.BinaryOperator;
import au.csiro.pathling.fhirpath.operator.BinaryOperatorInput;
import jakarta.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Value;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

final public class Paths {

  private Paths() {
  }

  /**
   * FHIRPath expression with a type specifier value.
   */
  @Value
  public static class TypeSpecifierPath implements FhirPath {

    TypeSpecifier typeSpecifier;

    @Override
    public Collection apply(@Nonnull final Collection input,
        @Nonnull final EvaluationContext context) {
      throw new UnsupportedOperationException("TypeSpecifierPath cannot be evaluated directly");
    }
  }

  @Value
  public static class ExternalConstantPath implements FhirPath {

    String name;

    @Override
    public Collection apply(@Nonnull final Collection input,
        @Nonnull final EvaluationContext context) {
      return context.resolveVariable(name);
    }


    @Nonnull
    @Override
    public String toExpression() {
      return name;
    }
  }

  @Value
  public static class EvalOperator implements FhirPath {

    @Nonnull
    FhirPath leftPath;

    @Nonnull
    FhirPath rightPath;

    @Nonnull
    BinaryOperator operator;

    @Override
    public Collection apply(@Nonnull final Collection input,
        @Nonnull final EvaluationContext context) {
      return operator.invoke(new BinaryOperatorInput(context, leftPath.apply(input, context),
          rightPath.apply(input, context)));
    }


    @Override
    @Nonnull
    public FhirPath withNewChildren(@Nonnull final List<FhirPath> newChildren) {
      if (newChildren.size() != 2) {
        throw new IllegalArgumentException("EvalOperator must have exactly two children");
      }
      return new EvalOperator(newChildren.get(0), newChildren.get(1), operator);
    }

    @Override
    public Stream<FhirPath> children() {
      return Stream.of(leftPath, rightPath);
    }
  }

  @Value
  public static class EvalFunction implements FhirPath {

    @Nonnull
    String functionIdentifier;

    @Nonnull
    List<FhirPath> arguments;

    @Override
    public Collection apply(@Nonnull final Collection input,
        @Nonnull final EvaluationContext context) {
      final NamedFunction<Collection> function;
      try {
        function = context.resolveFunction(functionIdentifier);
      } catch (final NoSuchFunctionException e) {
        throw new InvalidUserInputError(e.getMessage());
      }
      final FunctionInput functionInput = new FunctionInput(context, input, arguments);
      return function.invoke(functionInput);
    }


    @Nonnull
    @Override
    public String toExpression() {
      return functionIdentifier + "(" + arguments.stream().map(FhirPath::toExpression)
          .collect(Collectors.joining(",")) + ")";
    }


    @Override
    public Stream<FhirPath> children() {
      return arguments.stream();
    }

    @Override
    @Nonnull
    public FhirPath withNewChildren(@Nonnull final List<FhirPath> newChildren) {
      if (newChildren.size() != arguments.size()) {
        throw new IllegalArgumentException(
            "EvalFunction must have exactly " + arguments.size() + " children");
      }
      return new EvalFunction(functionIdentifier, Collections.unmodifiableList(newChildren));
    }
  }

  @Value
  public static class Traversal implements FhirPath {

    @Nonnull
    String propertyName;

    @Override
    public Collection apply(@Nonnull final Collection input,
        @Nonnull final EvaluationContext context) {
      final Optional<Collection> result = input.traverse(propertyName);
      checkUserInput(result.isPresent(), "No such child: " + propertyName);
      return result.get();
    }

    @Nonnull
    @Override
    public String toExpression() {
      return propertyName;
    }
  }

  @Value
  public static class Resource implements FhirPath {

    @Nonnull
    ResourceType resourceType;

    @Override
    public Collection apply(@Nonnull final Collection input,
        @Nonnull final EvaluationContext context) {
      return context.resolveResource(resourceType);
    }

    @Nonnull
    @Override
    public String toExpression() {
      return resourceType.toCode();
    }
  }


  @Value
  public static class Invocation implements FhirPath {

    FhirPath invocationSubject;
    FhirPath invocationVerb;

    @Override
    public Collection apply(@Nonnull final Collection input,
        @Nonnull final EvaluationContext context) {
      return invocationVerb.apply(invocationSubject.apply(input, context),
          context);
    }
  }

  // TODO: replace with FhirPath::This
  @Value
  public static class This implements FhirPath {

    @Override
    public Collection apply(@Nonnull final Collection input,
        @Nonnull final EvaluationContext context) {
      return input;
    }

    @Nonnull
    @Override
    public String toExpression() {
      return "$this";
    }
  }

  // TODO: Replace with Literal::StringLiteral
  @Value
  public static class StringLiteral implements FhirPath {

    String value;

    @Override
    public Collection apply(@Nonnull final Collection input,
        @Nonnull final EvaluationContext context) {
      return StringCollection.fromLiteral(value);
    }

    @Nonnull
    @Override
    public String toExpression() {
      // TODO: use a better conversion
      return "'" + value + "'";
    }
  }
}
