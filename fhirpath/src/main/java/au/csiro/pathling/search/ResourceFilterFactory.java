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
import au.csiro.pathling.fhirpath.execution.ColumnOnlyResolver;
import au.csiro.pathling.fhirpath.execution.FhirPathEvaluator;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.search.filter.SearchFilter;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import lombok.Value;
import org.apache.spark.sql.Column;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Factory for creating {@link ResourceFilter} instances from various sources.
 * <p>
 * This factory creates filters that can be applied to Pathling-encoded FHIR resource datasets.
 * Filters are created eagerly (SparkSQL Column expressions are built at creation time), allowing
 * them to be created without requiring a SparkSession or actual data.
 * <p>
 * The factory supports creating filters from:
 * <ul>
 *   <li>FHIR Search queries ({@link #fromSearch}, {@link #fromQueryString})</li>
 *   <li>FHIRPath expressions ({@link #fromExpression})</li>
 * </ul>
 * <p>
 * Usage example:
 * <pre>{@code
 * // Create factory with default registry
 * ResourceFilterFactory factory = ResourceFilterFactory.withDefaultRegistry(fhirContext);
 *
 * // Create filter from FHIR search query string
 * ResourceFilter genderFilter = factory.fromQueryString(ResourceType.PATIENT, "gender=male");
 *
 * // Create filter from FHIRPath expression
 * ResourceFilter activeFilter = factory.fromExpression(ResourceType.PATIENT, "active = true");
 *
 * // Combine filters
 * ResourceFilter combined = genderFilter.and(activeFilter);
 *
 * // Apply to dataset
 * Dataset<Row> result = combined.apply(dataSource.read("Patient"));
 * }</pre>
 *
 * @see ResourceFilter
 */
@Value
public class ResourceFilterFactory {

  /**
   * Resource path for the bundled R4 search parameters.
   */
  private static final String R4_REGISTRY_RESOURCE = "/fhir/R4/search-parameters.json";

  /**
   * The FHIR context for resource definitions.
   */
  @Nonnull
  FhirContext fhirContext;

  /**
   * The search parameter registry for looking up parameter definitions.
   */
  @Nonnull
  SearchParameterRegistry registry;

  /**
   * The FHIRPath parser for parsing expressions.
   */
  @Nonnull
  Parser parser;

  /**
   * Creates a factory with the default bundled search parameter registry for FHIR R4.
   * <p>
   * This method requires an R4 FhirContext and uses the bundled R4 search parameters from the HL7
   * FHIR specification.
   *
   * @param fhirContext the FHIR context (must be R4)
   * @return a new factory with the default R4 registry
   * @throws IllegalArgumentException if the FhirContext is not R4
   */
  @Nonnull
  public static ResourceFilterFactory withDefaultRegistry(@Nonnull final FhirContext fhirContext) {
    if (fhirContext.getVersion().getVersion() != FhirVersionEnum.R4) {
      throw new IllegalArgumentException(
          "Default registry requires FHIR R4 context, but got: "
              + fhirContext.getVersion().getVersion());
    }
    try (final InputStream is = ResourceFilterFactory.class.getResourceAsStream(
        R4_REGISTRY_RESOURCE)) {
      if (is == null) {
        throw new IllegalStateException(
            "Search parameters resource not found: " + R4_REGISTRY_RESOURCE);
      }
      return withRegistry(fhirContext, SearchParameterRegistry.fromInputStream(fhirContext, is));
    } catch (final IOException e) {
      throw new UncheckedIOException("Failed to load default search parameters", e);
    }
  }

  /**
   * Creates a factory with an explicit registry.
   *
   * @param fhirContext the FHIR context
   * @param registry the search parameter registry
   * @return a new factory
   */
  @Nonnull
  public static ResourceFilterFactory withRegistry(
      @Nonnull final FhirContext fhirContext,
      @Nonnull final SearchParameterRegistry registry) {
    return new ResourceFilterFactory(fhirContext, registry, new Parser());
  }

  /**
   * Creates a filter from a FHIR search query.
   * <p>
   * Multiple criteria in the search are combined with AND logic.
   *
   * @param resourceType the resource type to filter
   * @param search the FHIR search query
   * @return a filter representing the search criteria
   * @throws UnknownSearchParameterException if a search parameter is not found in the registry
   * @throws InvalidModifierException if an unsupported modifier is used
   * @throws InvalidSearchParameterException if the search parameter configuration is invalid
   */
  @Nonnull
  public ResourceFilter fromSearch(
      @Nonnull final ResourceType resourceType,
      @Nonnull final FhirSearch search) {

    // If there are no criteria, return a filter that matches all resources
    if (search.getCriteria().isEmpty()) {
      return new ResourceFilter(resourceType, lit(true));
    }

    // Create the evaluator for FHIRPath evaluation (using ColumnOnlyResolver)
    final FhirPathEvaluator evaluator = createEvaluator(resourceType);

    // Build the combined filter expression
    final Column filterExpression = search.getCriteria().stream()
        .map(criterion -> buildCriterionFilter(resourceType, criterion, evaluator))
        .reduce(Column::and)
        .orElse(lit(true));

    return new ResourceFilter(resourceType, filterExpression);
  }

  /**
   * Creates a filter from a FHIR search query string.
   * <p>
   * The query string should be in standard URL query format without the leading '?'.
   *
   * @param resourceType the resource type to filter
   * @param queryString the query string (e.g., "gender=male&amp;birthdate=ge1990")
   * @return a filter representing the search criteria
   * @throws UnknownSearchParameterException if a search parameter is not found in the registry
   * @throws InvalidModifierException if an unsupported modifier is used
   * @throws InvalidSearchParameterException if the search parameter configuration is invalid
   */
  @Nonnull
  public ResourceFilter fromQueryString(
      @Nonnull final ResourceType resourceType,
      @Nonnull final String queryString) {
    return fromSearch(resourceType, FhirSearch.fromQueryString(queryString));
  }

  /**
   * Creates a filter from a FHIRPath boolean expression.
   * <p>
   * The expression should evaluate to a boolean value. Resources where the expression evaluates to
   * true will be included in the filtered result.
   *
   * @param resourceType the resource type to filter
   * @param fhirPathExpression the FHIRPath expression (must evaluate to boolean)
   * @return a filter representing the expression
   */
  @Nonnull
  public ResourceFilter fromExpression(
      @Nonnull final ResourceType resourceType,
      @Nonnull final String fhirPathExpression) {

    // Parse the FHIRPath expression
    final FhirPath fhirPath = parser.parse(fhirPathExpression);

    // Create evaluator and evaluate
    final FhirPathEvaluator evaluator = createEvaluator(resourceType);
    final Collection result = evaluator.evaluate(fhirPath);

    // Get the Column value - for boolean expressions this should be a scalar boolean
    final Column filterExpression = result.getColumn().getValue();

    return new ResourceFilter(resourceType, filterExpression);
  }

  /**
   * Creates a FhirPathEvaluator using ColumnOnlyResolver.
   * <p>
   * This evaluator generates Column references without requiring a DataSource.
   *
   * @param resourceType the resource type
   * @return a new evaluator
   */
  @Nonnull
  private FhirPathEvaluator createEvaluator(@Nonnull final ResourceType resourceType) {
    return FhirPathEvaluator.fromResolver(
        new ColumnOnlyResolver(resourceType, fhirContext)).build();
  }

  /**
   * Builds a filter expression for a single search criterion.
   * <p>
   * For polymorphic search parameters with multiple expressions, each expression is evaluated
   * separately and the filter results are combined with OR logic.
   *
   * @param resourceType the resource type
   * @param criterion the search criterion
   * @param evaluator the FHIRPath evaluator
   * @return a SparkSQL Column expression for this criterion
   */
  @Nonnull
  private Column buildCriterionFilter(
      @Nonnull final ResourceType resourceType,
      @Nonnull final SearchCriterion criterion,
      @Nonnull final FhirPathEvaluator evaluator) {

    // Look up the parameter definition
    final SearchParameterDefinition paramDef = registry
        .getParameter(resourceType, criterion.getParameterCode())
        .orElseThrow(() -> new UnknownSearchParameterException(
            criterion.getParameterCode(), resourceType));

    // Build filter for each expression and combine with OR
    return paramDef.expressions().stream()
        .map(expression -> buildExpressionFilter(
            paramDef.type(), criterion, expression, evaluator))
        .reduce(Column::or)
        .orElse(lit(false));
  }

  /**
   * Builds a filter expression for a single FHIRPath expression within a search criterion.
   *
   * @param paramType the search parameter type
   * @param criterion the search criterion (for modifier and values)
   * @param expression the FHIRPath expression to evaluate
   * @param evaluator the FHIRPath evaluator
   * @return a SparkSQL Column expression for this expression
   */
  @Nonnull
  private Column buildExpressionFilter(
      @Nonnull final SearchParameterType paramType,
      @Nonnull final SearchCriterion criterion,
      @Nonnull final String expression,
      @Nonnull final FhirPathEvaluator evaluator) {

    // Parse the FHIRPath expression
    final FhirPath fhirPath = parser.parse(expression);

    // Evaluate the FHIRPath to extract the value column
    final Collection result = evaluator.evaluate(fhirPath);
    final ColumnRepresentation valueColumn = result.getColumn();

    // Get FHIR type from collection - fail if not available
    final FHIRDefinedType fhirType = result.getFhirType()
        .orElseThrow(() -> new InvalidSearchParameterException(
            "Cannot determine FHIR type for expression: " + expression));

    // Get the appropriate filter for the parameter type, modifier, and FHIR type
    final SearchFilter filter = getFilterForType(paramType, criterion.getModifier(), fhirType);

    // Build and return the filter expression
    return filter.buildFilter(valueColumn, criterion.getValues());
  }

  /**
   * Gets the appropriate filter implementation for a search parameter type, modifier, and FHIR
   * type.
   *
   * @param type the search parameter type
   * @param modifier the search modifier (e.g., "not", "exact"), or null for no modifier
   * @param fhirType the FHIR type of the element being searched
   * @return the filter implementation
   * @throws InvalidModifierException if the modifier is not supported for the parameter type
   * @throws InvalidSearchParameterException if the FHIR type is not valid for the search parameter
   *     type
   */
  @Nonnull
  private SearchFilter getFilterForType(
      @Nonnull final SearchParameterType type,
      @Nullable final String modifier,
      @Nonnull final FHIRDefinedType fhirType) {

    // Validate FHIR type is allowed for this search parameter type
    if (!type.isAllowedFhirType(fhirType)) {
      throw new InvalidSearchParameterException(
          String.format("FHIR type '%s' is not valid for search parameter type '%s'. "
              + "Allowed types: %s", fhirType.toCode(), type, type.getAllowedFhirTypes()));
    }

    // Delegate to enum constant's createFilter() implementation
    return type.createFilter(modifier, fhirType);
  }
}
