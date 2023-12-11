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
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.FunctionInput;
import au.csiro.pathling.fhirpath.PathEvalContext;
import au.csiro.pathling.fhirpath.TypeSpecifier;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.StringCollection;
import au.csiro.pathling.fhirpath.function.NamedFunction;
import au.csiro.pathling.fhirpath.function.registry.FunctionRegistry.NoSuchFunctionException;
import au.csiro.pathling.fhirpath.operator.BinaryOperator;
import au.csiro.pathling.fhirpath.operator.BinaryOperatorInput;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import ca.uhn.fhir.model.api.annotation.Block;
import lombok.Value;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

final public class Paths {

  private Paths() {
  }

  /**
   * FHIRPath expression with a type specifier value.
   */
  @Value
  public static class TypeSpecifierPath implements FhirPath<Collection> {

    TypeSpecifier typeSpecifier;

    @Override
    public Collection apply(@Nonnull final Collection input,
        @Nonnull final PathEvalContext context) {
      throw new UnsupportedOperationException("TypeSpecifierPath cannot be evaluated directly");
    }
  }

  @Value
  public static class ExtConsFhir implements FhirPath<Collection> {

    String name;

    @Override
    public Collection apply(@Nonnull final Collection input,
        @Nonnull final PathEvalContext context) {
      return context.resolveVariable(name);
    }


    @Nonnull
    @Override
    public String toExpression() {
      return name;
    }
  }

  @Value
  public static class EvalOperator implements FhirPath<Collection> {

    @Nonnull
    FhirPath<Collection> leftPath;

    @Nonnull
    FhirPath<Collection> rightPath;

    @Nonnull
    BinaryOperator operator;

    @Override
    public Collection apply(@Nonnull final Collection input,
        @Nonnull final PathEvalContext context) {
      return operator.invoke(new BinaryOperatorInput(context, leftPath.apply(input, context),
          rightPath.apply(input, context)));
    }


    @Override
    public Stream<FhirPath<Collection>> children() {
      return Stream.of(leftPath, rightPath);
    }
  }

  @Value
  public static class EvalFunction implements FhirPath<Collection> {

    @Nonnull
    String functionIdentifier;

    @Nonnull
    List<FhirPath<Collection>> arguments;

    @Override
    public Collection apply(@Nonnull final Collection input,
        @Nonnull final PathEvalContext context) {
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
    public Stream<FhirPath<Collection>> children() {
      return arguments.stream();
    }
  }

  @Value
  public static class Traversal implements FhirPath<Collection> {

    @Nonnull
    String propertyName;

    @Override
    public Collection apply(@Nonnull final Collection input,
        @Nonnull final PathEvalContext context) {
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
  public static class Resource implements FhirPath<Collection> {

    @Nonnull
    ResourceType resourceType;

    @Override
    public Collection apply(@Nonnull final Collection input,
        @Nonnull final PathEvalContext context) {
      return context.resolveResource(resourceType);
    }

    @Nonnull
    @Override
    public String toExpression() {
      return resourceType.toCode();
    }
  }


  @Value
  public static class Invocation implements FhirPath<Collection> {

    FhirPath<Collection> invocationSubject;
    FhirPath<Collection> invocationVerb;

    @Override
    public Collection apply(@Nonnull final Collection input,
        @Nonnull final PathEvalContext context) {
      return invocationVerb.apply(invocationSubject.apply(input, context),
          context);
    }
  }

  @Value
  public static class This implements FhirPath<Collection> {

    @Override
    public Collection apply(@Nonnull final Collection input,
        @Nonnull final PathEvalContext context) {
      return input;
    }
  }

  @Value
  public static class StringLiteral implements FhirPath<Collection> {

    String value;

    @Override
    public Collection apply(@Nonnull final Collection input,
        @Nonnull final PathEvalContext context) {
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
