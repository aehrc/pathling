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

package au.csiro.pathling.fhirpath.operator.comparison;

import au.csiro.pathling.fhirpath.collection.Collection;

/**
 * Describes a set of methods that can be used to compare {@link Collection} objects to other paths,
 * e.g. for equality.
 *
 * @author John Grimes
 * @author Piotr Szul
 */
public interface Comparable {

  // ColumnComparator DEFAULT_COMPARATOR = new DefaultComparator();
  //
  // /**
  //  * Builds a comparison function for directly comparable paths using the custom comparator.
  //  *
  //  * @param source The path to build the comparison function for
  //  * @param operation The {@link ComparisonOperation} type to retrieve a comparison for
  //  * @param comparator The {@link ColumnComparator} to use
  //  * @return A new {@link Function}
  //  */
  // @Nonnull
  // static Function<Comparable, Column> buildComparison(@Nonnull final Comparable source,
  //     @Nonnull final ComparisonOperation operation, @Nonnull final ColumnComparator comparator) {
  //
  //   final TriFunction<ColumnComparator, Column, Column, Column> compFunction = operation.compFunction;
  //
  //   // TODO: Move to the interface (asking for the singular column)
  //   return target -> compFunction
  //       .apply(comparator, ColumnHelpers.singular(source.getColumn().getValue()),
  //           ColumnHelpers.singular(target.getColumn().getValue()));
  // }
  //
  // /**
  //  * Builds a comparison function for directly comparable paths using the standard Spark SQL
  //  * comparison operators.
  //  *
  //  * @param source The path to build the comparison function for
  //  * @param operation The {@link ComparisonOperation} type to retrieve a comparison for
  //  * @return A new {@link Function}
  //  */
  // static Function<Comparable, Column> buildComparison(@Nonnull final Comparable source,
  //     @Nonnull final ComparisonOperation operation) {
  //
  //   return buildComparison(source, operation, DEFAULT_COMPARATOR);
  // }
  //
  // /**
  //  * Get a function that can take two Comparable paths and return a {@link Column} that contains a
  //  * comparison condition. The type of condition is controlled by supplying a
  //  * {@link ComparisonOperation}.
  //  * <p>
  //  * Please use {@link #isComparableTo(Collection)} to first check whether the specified path should
  //  * be compared to this path.
  //  *
  //  * @param operation The {@link ComparisonOperation} type to retrieve a comparison for
  //  * @return A {@link Function} that takes a Comparable as its parameter, and returns a
  //  * {@link Column}
  //  */
  // @Nonnull
  // Function<Comparable, Column> getComparison(@Nonnull ComparisonOperation operation);
  //
  // /**
  //  * Returns a {@link Column} within the dataset containing the values of the nodes.
  //  *
  //  * @return A {@link Column}
  //  */
  // @Nonnull
  // ColumnRepresentation getColumn();
  //
  // /**
  //  * @return {@code true} if this path can be compared to the specified class
  //  */
  // boolean isComparableTo(@Nonnull Collection path);

}
