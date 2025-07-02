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

import au.csiro.pathling.errors.UnsupportedFhirPathFeatureError;
import au.csiro.pathling.fhirpath.EvaluationContext;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.EmptyCollection;
import au.csiro.pathling.fhirpath.function.FunctionInput;
import au.csiro.pathling.fhirpath.function.NamedFunction;
import au.csiro.pathling.fhirpath.function.registry.NoSuchFunctionException;
import au.csiro.pathling.fhirpath.operator.BinaryOperator;
import au.csiro.pathling.fhirpath.operator.BinaryOperatorInput;
import au.csiro.pathling.fhirpath.operator.BinaryOperatorType;
import au.csiro.pathling.fhirpath.operator.UnaryOperator;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Value;
import lombok.experimental.UtilityClass;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

@UtilityClass
public final class Paths {

  /**
   * Gets the `$this` path.
   * <p>
   * `$this`   is now represented by the nullPath(). The implication is that  `$this`  will only
   * appear in fhir paths is explicitly needed. In all cases when $this is followed another path
   * element it is stripped.
   * <p>
   * For example:
   * <pre>`where($this.name = 'foo')` is converted to `where(name = 'foo')`</pre>
   * but:
   * <pre>`where($this = 'foo')` remains `where($this = 'foo')`</pre>
   *
   * @return the `$this` path
   */
  public static FhirPath thisPath() {
    return FhirPath.nullPath();
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
    public Stream<FhirPath> children() {
      return Stream.of(leftPath, rightPath);
    }

    @Override
    @Nonnull
    public String toExpression() {
      return argToExpression(leftPath)
          + " "
          + operator.getOperatorName()
          + " "
          + argToExpression(rightPath);
    }

    @Override
    @Nonnull
    public String toTermExpression() {
      return "(" + toExpression() + ")";
    }

    @Nonnull
    private String argToExpression(@Nonnull final FhirPath arg) {
      if (arg instanceof EvalOperator opArg) {
        return BinaryOperatorType.comparePrecedence(operator.getOperatorName(),
            opArg.operator.getOperatorName()) >= 0
               ? arg.toExpression()
               : arg.toTermExpression();
      } else {
        return arg.toExpression();
      }
    }

    @Override
    @Nonnull
    public FhirPath prefix() {
      return leftPath.isNull()
             ? this
             : leftPath.head();
    }

  }

  @Value
  public static class EvalUnaryOperator implements FhirPath {

    @Nonnull
    FhirPath path;

    @Nonnull
    UnaryOperator operator;

    @Override
    public Collection apply(@Nonnull final Collection input,
        @Nonnull final EvaluationContext context) {
      return operator.invoke(new UnaryOperator.UnaryOperatorInput(path.apply(input, context)));
    }

    @Override
    @Nonnull
    public String toExpression() {
      return operator.getOperatorName() + path.toTermExpression();
    }

    @Override
    public Stream<FhirPath> children() {
      return Stream.of(path);
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
        throw new UnsupportedFhirPathFeatureError(e.getMessage());
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

  }

  @Value
  public static class Traversal implements FhirPath {

    @Nonnull
    String propertyName;

    @Override
    public Collection apply(@Nonnull final Collection input,
        @Nonnull final EvaluationContext context) {
      return input.traverse(propertyName).orElse(EmptyCollection.getInstance());
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
    String resourceCode;

    @Nonnull
    public ResourceType getResourceType() {
      return ResourceType.fromCode(resourceCode);
    }

    @Override
    public Collection apply(@Nonnull final Collection input,
        @Nonnull final EvaluationContext context) {
      return context.resolveResource(resourceCode)
          .map(Collection.class::cast)
          .orElseGet(() -> new Traversal(resourceCode).apply(input, context));
    }

    @Nonnull
    @Override
    public String toExpression() {
      return resourceCode;
    }
  }

}
