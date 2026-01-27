/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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
import au.csiro.pathling.fhirpath.function.registry.NoSuchFunctionError;
import au.csiro.pathling.fhirpath.operator.BinaryOperatorType;
import au.csiro.pathling.fhirpath.operator.FhirPathBinaryOperator;
import au.csiro.pathling.fhirpath.operator.UnaryOperator;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.experimental.UtilityClass;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Utility class containing factory methods and helper classes for FHIRPath operations.
 *
 * @author John Grimes
 */
@UtilityClass
public final class Paths {

  /**
   * Gets the `$this` path.
   *
   * <p>`$this` is now represented by the nullPath(). The implication is that `$this` will only
   * appear in fhir paths is explicitly needed. In all cases when $this is followed another path
   * element it is stripped.
   *
   * <p>For example:
   *
   * <pre>`where($this.name = 'foo')` is converted to `where(name = 'foo')`</pre>
   *
   * but:
   *
   * <pre>`where($this = 'foo')` remains `where($this = 'foo')`</pre>
   *
   * @return the `$this` path
   */
  public static FhirPath thisPath() {
    return FhirPath.nullPath();
  }

  /**
   * Represents an external constant path in FHIRPath expressions.
   *
   * @param name the name of the constant
   */
  public record ExternalConstantPath(String name) implements FhirPath {

    @Nonnull
    @Override
    public Collection apply(
        @Nonnull final Collection input, @Nonnull final EvaluationContext context) {
      return context.resolveVariable(name);
    }

    @Nonnull
    @Override
    public String toExpression() {
      return name;
    }
  }

  /**
   * Represents a binary operator evaluation in FHIRPath expressions.
   *
   * @param leftPath the left operand path
   * @param rightPath the right operand path
   * @param operator the binary operator to apply
   */
  public record EvalOperator(
      @Nonnull FhirPath leftPath,
      @Nonnull FhirPath rightPath,
      @Nonnull FhirPathBinaryOperator operator)
      implements FhirPath {

    @Nonnull
    @Override
    public Collection apply(
        @Nonnull final Collection input, @Nonnull final EvaluationContext context) {
      // Delegate to the operator to decide how to evaluate paths
      return operator.invokeWithPaths(context, input, leftPath, rightPath);
    }

    @Nonnull
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
      if (arg instanceof final EvalOperator opArg) {
        return BinaryOperatorType.comparePrecedence(
                    operator.getOperatorName(), opArg.operator.getOperatorName())
                >= 0
            ? arg.toExpression()
            : arg.toTermExpression();
      } else {
        return arg.toExpression();
      }
    }
  }

  /**
   * Represents a unary operator evaluation in FHIRPath expressions.
   *
   * @param path the operand path
   * @param operator the unary operator to apply
   */
  public record EvalUnaryOperator(@Nonnull FhirPath path, @Nonnull UnaryOperator operator)
      implements FhirPath {

    @Nonnull
    @Override
    public Collection apply(
        @Nonnull final Collection input, @Nonnull final EvaluationContext context) {
      return operator.invoke(new UnaryOperator.UnaryOperatorInput(path.apply(input, context)));
    }

    @Override
    @Nonnull
    public String toExpression() {
      return operator.getOperatorName() + path.toTermExpression();
    }

    @Nonnull
    @Override
    public Stream<FhirPath> children() {
      return Stream.of(path);
    }
  }

  /**
   * Represents a function evaluation in FHIRPath expressions.
   *
   * @param functionIdentifier the identifier of the function to evaluate
   * @param arguments the list of arguments to the function
   */
  public record EvalFunction(@Nonnull String functionIdentifier, @Nonnull List<FhirPath> arguments)
      implements FhirPath {

    @Override
    public Collection apply(
        @Nonnull final Collection input, @Nonnull final EvaluationContext context) {
      final NamedFunction function;
      try {
        function = context.resolveFunction(functionIdentifier);
      } catch (final NoSuchFunctionError e) {
        throw new UnsupportedFhirPathFeatureError(e.getMessage());
      }
      final FunctionInput functionInput = new FunctionInput(context, input, arguments);
      return function.invoke(functionInput);
    }

    @Nonnull
    @Override
    public String toExpression() {
      return functionIdentifier
          + "("
          + arguments.stream().map(FhirPath::toExpression).collect(Collectors.joining(","))
          + ")";
    }

    @Override
    public Stream<FhirPath> children() {
      return arguments.stream();
    }
  }

  /**
   * Represents a property traversal in FHIRPath expressions.
   *
   * @param propertyName the name of the property to traverse
   */
  public record Traversal(@Nonnull String propertyName) implements FhirPath {

    @Override
    public Collection apply(
        @Nonnull final Collection input, @Nonnull final EvaluationContext context) {
      return input.traverse(propertyName).orElse(EmptyCollection.getInstance());
    }

    @Nonnull
    @Override
    public String toExpression() {
      return propertyName;
    }
  }

  /**
   * Represents a FHIR resource type in FHIRPath expressions.
   *
   * @param resourceCode the code of the resource type
   */
  public record Resource(@Nonnull String resourceCode) implements FhirPath {

    /**
     * Gets the resource type for this resource path.
     *
     * @return the resource type
     */
    @Nonnull
    public ResourceType getResourceType() {
      return ResourceType.fromCode(resourceCode);
    }

    @Override
    public Collection apply(
        @Nonnull final Collection input, @Nonnull final EvaluationContext context) {
      return context
          .resolveResource(resourceCode)
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
