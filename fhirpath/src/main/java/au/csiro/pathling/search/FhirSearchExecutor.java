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

package au.csiro.pathling.search;

import static org.apache.spark.sql.functions.lit;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import au.csiro.pathling.fhirpath.execution.FhirPathEvaluator;
import au.csiro.pathling.fhirpath.execution.FhirPathEvaluators.SingleEvaluatorFactory;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.search.filter.ExactStringMatcher;
import au.csiro.pathling.search.filter.SearchFilter;
import au.csiro.pathling.search.filter.StringMatcher;
import au.csiro.pathling.search.filter.TokenMatcher;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Executes FHIR search queries against a data source.
 * <p>
 * This executor translates FHIR search criteria into FHIRPath expressions to extract values from
 * resources, then builds SparkSQL filter expressions to filter the resources based on the search
 * values.
 *
 * @see <a href="https://hl7.org/fhir/search.html">FHIR Search</a>
 */
@Getter
@RequiredArgsConstructor
public class FhirSearchExecutor {

  @Nonnull
  private final FhirContext fhirContext;

  @Nonnull
  private final DataSource dataSource;

  @Nonnull
  private final SearchParameterRegistry registry;

  @Nonnull
  private final Parser parser;

  /**
   * Creates a new FhirSearchExecutor with a default parser and registry.
   *
   * @param fhirContext the FHIR context
   * @param dataSource the data source containing FHIR resources
   */
  public FhirSearchExecutor(@Nonnull final FhirContext fhirContext,
      @Nonnull final DataSource dataSource) {
    this(fhirContext, dataSource, new SearchParameterRegistry(), new Parser());
  }

  /**
   * Executes a FHIR search query.
   *
   * @param resourceType the resource type to search
   * @param search the search criteria
   * @return a filtered dataset with the same schema as the original resource data
   */
  @Nonnull
  public Dataset<Row> execute(@Nonnull final ResourceType resourceType,
      @Nonnull final FhirSearch search) {
    // If there are no criteria, return all resources
    if (search.getCriteria().isEmpty()) {
      return dataSource.read(resourceType.toCode());
    }

    // Create the evaluator factory for FHIRPath evaluation
    final FhirPathEvaluator.Factory evaluatorFactory =
        SingleEvaluatorFactory.of(fhirContext, dataSource);

    // Create the evaluator and get its initial dataset
    // The evaluator's dataset has a specific structure where the resource is wrapped
    // in a struct column named after the resource type (e.g., "Patient")
    final FhirPathEvaluator evaluator = evaluatorFactory.create(resourceType);
    final Dataset<Row> evaluatorDataset = evaluator.createInitialDataset();

    // Build the combined filter expression
    final Column filterExpression = buildFilterExpression(resourceType, search, evaluatorFactory);

    // Apply the filter to the evaluator's dataset
    final Dataset<Row> filteredDataset = evaluatorDataset.filter(filterExpression);

    // Transform back to flat schema by selecting the nested struct fields
    // The evaluator dataset has: id, key, ResourceType (struct with all fields)
    final String resourceCode = resourceType.toCode();
    return filteredDataset.select(resourceCode + ".*");
  }

  /**
   * Builds a combined filter expression for all search criteria.
   *
   * @param resourceType the resource type
   * @param search the search criteria
   * @param evaluatorFactory the evaluator factory for FHIRPath evaluation
   * @return a SparkSQL Column expression that combines all criteria with AND logic
   */
  @Nonnull
  private Column buildFilterExpression(
      @Nonnull final ResourceType resourceType,
      @Nonnull final FhirSearch search,
      @Nonnull final FhirPathEvaluator.Factory evaluatorFactory) {

    // Combine all criteria with AND logic
    return search.getCriteria().stream()
        .map(criterion -> buildCriterionFilter(resourceType, criterion, evaluatorFactory))
        .reduce(Column::and)
        .orElse(lit(true));
  }

  /**
   * Builds a filter expression for a single search criterion.
   *
   * @param resourceType the resource type
   * @param criterion the search criterion
   * @param evaluatorFactory the evaluator factory for FHIRPath evaluation
   * @return a SparkSQL Column expression for this criterion
   */
  @Nonnull
  private Column buildCriterionFilter(
      @Nonnull final ResourceType resourceType,
      @Nonnull final SearchCriterion criterion,
      @Nonnull final FhirPathEvaluator.Factory evaluatorFactory) {

    // Look up the parameter definition
    final SearchParameterDefinition paramDef = registry
        .getParameter(resourceType, criterion.getParameterCode())
        .orElseThrow(() -> new UnknownSearchParameterException(
            criterion.getParameterCode(), resourceType));

    // Parse the FHIRPath expression
    final FhirPath fhirPath = parser.parse(paramDef.getExpression());

    // Evaluate the FHIRPath to extract the value column
    final FhirPathEvaluator evaluator = evaluatorFactory.create(resourceType);
    final Collection result = evaluator.evaluate(fhirPath);
    final ColumnRepresentation valueColumn = result.getColumn();

    // Get the appropriate filter builder for the parameter type and modifier
    final SearchFilter filter = getFilterForType(paramDef.getType(), criterion.getModifier());

    // Build and return the filter expression
    return filter.buildFilter(valueColumn, criterion.getValues());
  }

  /**
   * Gets the appropriate filter implementation for a search parameter type and modifier.
   *
   * @param type the search parameter type
   * @param modifier the search modifier (e.g., "not", "exact"), or null for no modifier
   * @return the filter implementation
   * @throws InvalidModifierException if the modifier is not supported for the parameter type
   */
  @Nonnull
  private SearchFilter getFilterForType(@Nonnull final SearchParameterType type,
      @Nullable final String modifier) {
    return switch (type) {
      case TOKEN -> {
        if ("not".equals(modifier)) {
          yield new SearchFilter(new TokenMatcher(), true);
        }
        if (modifier != null) {
          throw new InvalidModifierException(modifier, type);
        }
        yield new SearchFilter(new TokenMatcher());
      }
      case STRING -> {
        if ("exact".equals(modifier)) {
          yield new SearchFilter(new ExactStringMatcher());
        }
        if (modifier != null) {
          throw new InvalidModifierException(modifier, type);
        }
        yield new SearchFilter(new StringMatcher());
      }
      default -> throw new UnsupportedOperationException(
          "Search parameter type not yet supported: " + type);
    };
  }
}
