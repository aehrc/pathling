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

import static java.util.stream.Collectors.toList;

import au.csiro.pathling.config.QueryConfiguration;
import au.csiro.pathling.fhirpath.Materializable;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.io.Database;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Type;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * This class knows how to take an {@link AggregateRequest} and execute it, returning the result as
 * an {@link AggregateResponse}.
 *
 * @author John Grimes
 */
@Component
@Profile("core")
@Slf4j
public class AggregateExecutor extends AggregateQueryExecutor {

  /**
   * @param configuration A {@link QueryConfiguration} object to control the behaviour of the
   * executor
   * @param fhirContext A {@link FhirContext} for doing FHIR stuff
   * @param sparkSession A {@link SparkSession} for resolving Spark queries
   * @param dataSource A {@link Database} for retrieving resources
   * @param terminologyServiceFactory A {@link TerminologyServiceFactory} for resolving terminology
   */
  public AggregateExecutor(@Nonnull final QueryConfiguration configuration,
      @Nonnull final FhirContext fhirContext, @Nonnull final SparkSession sparkSession,
      @Nonnull final DataSource dataSource,
      @Nonnull final Optional<TerminologyServiceFactory> terminologyServiceFactory) {
    super(configuration, fhirContext, sparkSession, dataSource,
        terminologyServiceFactory);
  }

  /**
   * @param query an {@link AggregateRequest}
   * @return the resulting {@link AggregateResponse}
   */
  @Nonnull
  public AggregateResponse execute(@Nonnull final AggregateRequest query) {
    final ResultWithExpressions resultWithExpressions = buildQuery(
        query);
    resultWithExpressions.getDataset().explain();
    resultWithExpressions.getDataset().show(1_000, false);

    // Translate the result into a response object to be passed back to the user.
    return buildResponse(resultWithExpressions);
  }

  @Nonnull
  private AggregateResponse buildResponse(
      @Nonnull final ResultWithExpressions resultWithExpressions) {
    // If explain queries is on, print out a query plan to the log.
    if (getConfiguration().getExplainQueries()) {
      log.debug("$aggregate query plan:");
      resultWithExpressions.getDataset().explain(true);
    }

    // Execute the query.
    final List<Row> rows = resultWithExpressions.getDataset().collectAsList();

    // Map each of the rows in the result to a grouping in the response object.
    final List<AggregateResponse.Grouping> groupings = rows.stream()
        .map(mapRowToGrouping(resultWithExpressions.getParsedAggregations(),
            resultWithExpressions.getParsedGroupings(),
            resultWithExpressions.getParsedFilters()))
        .collect(toList());

    return new AggregateResponse(groupings);
  }

  @Nonnull
  @SuppressWarnings("unchecked")
  private Function<Row, AggregateResponse.Grouping> mapRowToGrouping(
      @Nonnull final List<Collection> aggregations, @Nonnull final List<Collection> groupings,
      @Nonnull final java.util.Collection<Collection> filters) {
    return row -> {
      final List<Optional<Type>> labels = new ArrayList<>();
      final List<Optional<Type>> results = new ArrayList<>();

      for (int i = 0; i < groupings.size(); i++) {
        final Materializable<Type> grouping = (Materializable<Type>) groupings.get(i);
        // Delegate to the `getValueFromRow` method within each Materializable path class to extract 
        // the Type value from the Row in the appropriate way.
        final Optional<Type> label = grouping.getFhirValueFromRow(row, i);
        labels.add(label);
      }

      for (int i = 0; i < aggregations.size(); i++) {
        //noinspection rawtypes
        final Materializable aggregation = (Materializable<Type>) aggregations.get(i);
        // Delegate to the `getValueFromRow` method within each Materializable path class to extract 
        // the Type value from the Row in the appropriate way.
        final Optional<Type> result = aggregation.getFhirValueFromRow(row, i + groupings.size());
        results.add(result);
      }

      // Build a drill-down FHIRPath expression for inclusion with the returned grouping.
      final Optional<String> drillDown = new DrillDownBuilder(labels, groupings, filters).build();

      return new AggregateResponse.Grouping(labels, results, drillDown);
    };
  }

}
