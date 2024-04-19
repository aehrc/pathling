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

package au.csiro.pathling.aggregate;

import static au.csiro.pathling.utilities.Preconditions.checkArgument;
import static au.csiro.pathling.utilities.Strings.parentheses;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.literal.LiteralPath;
import au.csiro.pathling.utilities.Strings;
import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.hl7.fhir.r4.model.Type;

/**
 * Builds FHIRPath expressions for retrieving the resources that are members of a grouping returned
 * by the "aggregate" operation.
 *
 * @author John Grimes
 */
public class DrillDownBuilder {

  @Nonnull
  private final List<Optional<Type>> labels;

  @Nonnull
  private final List<FhirPath> groupings;

  @Nonnull
  private final Collection<FhirPath> filters;

  /**
   * @param labels The labels from the subject grouping
   * @param groupings The grouping expressions from the request
   * @param filters The filters from the request
   */
  public DrillDownBuilder(@Nonnull final List<Optional<Type>> labels,
      @Nonnull final List<FhirPath> groupings, @Nonnull final Collection<FhirPath> filters) {
    checkArgument(labels.size() == groupings.size(), "Labels should be same size as groupings");
    this.labels = labels;
    this.groupings = groupings;
    this.filters = filters;
  }

  /**
   * Generates the FHIRPath string.
   *
   * @return A FHIRPath expression, unless there are no groupings or filters
   */
  @Nonnull
  public Optional<String> build() {
    // We use a Set here to avoid situations where we needlessly have the same condition in the
    // expression more than once.
    final Collection<String> fhirPaths = new LinkedHashSet<>();

    // Add each of the grouping expressions, along with either equality or contains against the
    // group value to convert it in to a Boolean expression.
    addGroupings(fhirPaths);

    // Add each of the filter expressions.
    addFilters(fhirPaths);

    // If there is more than one expression, wrap each expression in parentheses before joining
    // together with Boolean AND operators.
    return fhirPaths.size() > 0
           ? Optional.of(String.join(" and ", parenthesiseExpressions(fhirPaths)))
           : Optional.empty();
  }

  private void addGroupings(final Collection<String> fhirPaths) {
    for (int i = 0; i < groupings.size(); i++) {
      final FhirPath grouping = groupings.get(i);
      final Optional<Type> label = labels.get(i);
      if (label.isPresent()) {
        final String literal = LiteralPath
            .expressionFor(grouping.getDataset(), grouping.getIdColumn(), label.get());
        final String equality = grouping.isSingular()
                                ? " = "
                                : " contains ";
        // We need to add parentheses around the grouping expression, as some expressions will not
        // play well with the equality or membership operator due to precedence.
        final String expression = literal.equals("true") && grouping.isSingular()
                                  ? grouping.getExpression()
                                  : parentheses(grouping.getExpression()) + equality + literal;
        fhirPaths.add(expression);
      } else {
        fhirPaths.add(parentheses(grouping.getExpression()) + ".empty()");
      }
    }
  }

  private void addFilters(@Nonnull final Collection<String> fhirPaths) {
    final List<String> filterExpressions = filters.stream()
        .map(FhirPath::getExpression)
        .collect(Collectors.toList());
    fhirPaths.addAll(filterExpressions);
  }

  @Nonnull
  private List<String> parenthesiseExpressions(@Nonnull final Collection<String> fhirPaths) {
    if (fhirPaths.size() > 1) {
      return fhirPaths.stream()
          .map(Strings::parentheses)
          .collect(Collectors.toList());
    } else {
      return new ArrayList<>(fhirPaths);
    }
  }

}
