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

package au.csiro.pathling.fhirpath;

import static au.csiro.pathling.utilities.Preconditions.checkArgument;

import au.csiro.pathling.fhirpath.collection.Collection;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Value;

/**
 * A description of how to take one {@link Collection} and transform it into another.
 *
 * @author John Grimes
 */
@FunctionalInterface
public interface FhirPath {

  /** A special FHIRPath that doesn't do anything. */
  FhirPath NULL = new This();

  /**
   * Applies this FHIRPath to the given input collection.
   *
   * @param input the input collection to transform
   * @param context the evaluation context
   * @return the transformed collection
   */
  Collection apply(@Nonnull final Collection input, @Nonnull final EvaluationContext context);

  /**
   * Chains this FHIRPath with another to create a composite transformation.
   *
   * @param after the FHIRPath to apply after this one
   * @return a composite FHIRPath that applies both transformations
   */
  default FhirPath andThen(@Nonnull final FhirPath after) {
    return nullPath().equals(after)
        ? this
        : new Composite(Stream.concat(asStream(), after.asStream()).toList());
  }

  /**
   * Converts this FHIRPath to a stream representation.
   *
   * @return a stream containing this FHIRPath element
   */
  default Stream<FhirPath> asStream() {
    return Stream.of(this);
  }

  /**
   * Returns the child FHIRPath elements of this path.
   *
   * @return a stream of child FHIRPath elements
   */
  default Stream<FhirPath> children() {
    return Stream.empty();
  }

  /**
   * Returns the null FHIRPath instance.
   *
   * @return the null FHIRPath
   */
  static FhirPath nullPath() {
    return NULL;
  }

  /**
   * Checks if this FHIRPath is the null path, which does not perform any transformation.
   *
   * @return true if this is the null path, false otherwise
   */
  default boolean isNull() {
    return NULL.equals(this);
  }

  /**
   * Converts this FHIRPath to its expression string representation.
   *
   * @return the FHIRPath expression as a string
   */
  @Nonnull
  default String toExpression() {
    return toString();
  }

  /**
   * Converts the FHIRPath expression that can be uses a term in a FHIRPath expression.
   *
   * @return the FHIRPath expression
   */
  @Nonnull
  default String toTermExpression() {
    return toExpression();
  }

  /**
   * Creates a composite FHIRPath from a list of FHIRPath elements.
   *
   * @param elements the list of FHIRPath elements
   * @return a FHIRPath representing the composition of the elements
   */
  @Nonnull
  static FhirPath of(@Nonnull final List<FhirPath> elements) {
    return switch (elements.size()) {
      case 0 -> nullPath();
      case 1 -> elements.get(0);
      default -> new Composite(elements);
    };
  }

  /** Implementation of FHIRPath that represents the identity transformation. */
  @Value
  class This implements FhirPath {

    @Override
    public Collection apply(
        @Nonnull final Collection input, @Nonnull final EvaluationContext context) {
      return input;
    }

    @Override
    public Stream<FhirPath> asStream() {
      return Stream.empty();
    }

    @Override
    public FhirPath andThen(@Nonnull final FhirPath after) {
      return after;
    }

    @Nonnull
    @Override
    public String toExpression() {
      return "$this";
    }
  }

  /**
   * Implementation of FHIRPath that represents a composition of multiple FHIRPath elements.
   *
   * @param elements the list of FHIRPath elements to compose
   */
  record Composite(@Nonnull List<FhirPath> elements) implements FhirPath {

    /**
     * Compact constructor that validates the elements list.
     *
     * @param elements the list of FHIRPath elements to compose
     */
    public Composite {
      checkArgument(elements.size() >= 2, "Composite must have at least two elements");
    }

    @Override
    public Collection apply(
        @Nonnull final Collection input, @Nonnull final EvaluationContext context) {
      return elements.stream()
          .reduce(input, (acc, element) -> element.apply(acc, context), (a, b) -> b);
    }

    @Nonnull
    @Override
    public Stream<FhirPath> asStream() {
      return elements.stream();
    }

    @Nonnull
    @Override
    public String toExpression() {
      return elements.stream().map(FhirPath::toTermExpression).collect(Collectors.joining("."));
    }
  }
}
