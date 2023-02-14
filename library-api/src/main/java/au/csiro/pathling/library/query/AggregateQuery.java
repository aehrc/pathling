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

package au.csiro.pathling.library.query;

import static au.csiro.pathling.utilities.Preconditions.requireNonBlank;

import au.csiro.pathling.aggregate.AggregateRequest;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import au.csiro.pathling.utilities.Lists;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Represents an aggregate  query.
 *
 * @author Piotr Szul
 */
public class AggregateQuery extends AbstractQueryWithFilters<AggregateQuery> {

  @Nonnull
  final List<String> groupings = new ArrayList<>();

  @Nonnull
  final List<String> aggregations = new ArrayList<>();

  private AggregateQuery(@Nonnull final ResourceType subjectResource) {
    super(subjectResource);
  }

  @Nonnull
  @Override
  protected Dataset<Row> doExecute(@Nonnull final PathlingClient pathlingClient) {
    return pathlingClient.execute(buildRequest());
  }
  
  /**
   * Adds a fhirpath expression that represents an aggregation grouping column..
   *
   * @param groupingFhirpath the column expressions.
   * @return this query.
   */
  @Nonnull
  public AggregateQuery withGrouping(@Nonnull final String groupingFhirpath) {
    groupings.add(requireNonBlank(groupingFhirpath, "Grouping expression cannot be blank"));
    return this;
  }

  /**
   * Adds a fhirpath expression that represents an aggregation column..
   *
   * @param aggregationFhirpath the column expressions.
   * @return this query.
   */
  @Nonnull
  public AggregateQuery withAggregation(@Nonnull final String aggregationFhirpath) {
    aggregations.add(
        requireNonBlank(aggregationFhirpath, "Aggregation expression cannot be blank"));
    return this;
  }

  /**
   * Construct a new extract query instance for the given subject resource type.
   *
   * @param subjectResourceType the type of the subject resource.
   * @return the new instance of (unbound) extract query.
   */
  @Nonnull
  public static AggregateQuery of(@Nonnull final ResourceType subjectResourceType) {
    return new AggregateQuery(subjectResourceType);
  }
  
  @Nonnull
  private AggregateRequest buildRequest() {
    return new AggregateRequest(subjectResource,
        Lists.normalizeEmpty(aggregations),
        Lists.normalizeEmpty(groupings),
        Lists.normalizeEmpty(filters));
  }
}
