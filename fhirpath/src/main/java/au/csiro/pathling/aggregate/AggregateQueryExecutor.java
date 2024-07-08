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

import au.csiro.pathling.QueryExecutor;
import au.csiro.pathling.config.QueryConfiguration;
import au.csiro.pathling.fhirpath.annotations.NotImplemented;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.io.Database;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Builds the overall query responsible for executing an aggregate request.
 *
 * @author John Grimes
 */
@Slf4j
@NotImplemented
public class AggregateQueryExecutor extends QueryExecutor {

  /**
   * Constructs a new {@link AggregateQueryExecutor}.
   *
   * @param configuration A {@link QueryConfiguration} object to control the behaviour of the
   * executor
   * @param fhirContext A {@link FhirContext} for doing FHIR stuff
   * @param sparkSession A {@link SparkSession} for resolving Spark queries
   * @param dataSource A {@link Database} for retrieving resources
   * @param terminologyServiceFactory A {@link TerminologyServiceFactory} for resolving terminology
   */
  public AggregateQueryExecutor(@Nonnull final QueryConfiguration configuration,
      @Nonnull final FhirContext fhirContext, @Nonnull final SparkSession sparkSession,
      @Nonnull final DataSource dataSource,
      @Nonnull final Optional<TerminologyServiceFactory> terminologyServiceFactory) {
    super(configuration, fhirContext, sparkSession, dataSource, terminologyServiceFactory);
  }

  /**
   * @param query an {@link AggregateRequest}
   * @return a {@link ResultWithExpressions}, which includes the uncollected {@link Dataset}
   */
  @SuppressWarnings("WeakerAccess")
  @Nonnull
  public ResultWithExpressions buildQuery(@Nonnull final AggregateRequest query) {
    return new ResultWithExpressions(sparkSession.emptyDataFrame(), Collections.emptyList(),
        Collections.emptyList(), Collections.emptyList());
  }

  @Value
  public static class ResultWithExpressions {

    @Nonnull
    Dataset<Row> dataset;

    @Nonnull
    List<Collection> parsedAggregations;

    @Nonnull
    List<Collection> parsedGroupings;

    @Nonnull
    java.util.Collection<Collection> parsedFilters;

  }
}
