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

import static au.csiro.pathling.utilities.Strings.randomAlias;

import au.csiro.pathling.QueryExecutor;
import au.csiro.pathling.config.QueryConfiguration;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.execution.FhirpathEvaluator;
import au.csiro.pathling.fhirpath.execution.MultiFhirpathEvaluator.ManyFactory;
import au.csiro.pathling.fhirpath.execution.SingleFhirpathEvaluator;
import au.csiro.pathling.fhirpath.function.registry.StaticFunctionRegistry;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.fhirpath.path.Paths;
import au.csiro.pathling.io.Database;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.sql.SqlExpressions;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.ArrayType;

/**
 * Builds the overall query responsible for executing an aggregate request.
 *
 * @author John Grimes
 */
@Slf4j
public class AggregateQueryExecutor extends QueryExecutor {

  @Nonnull
  private final Parser parser = new Parser();

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

    // TODO: I think the current implementation does not do implicit unnesting of grouping and aggregation expressions
    // but that needs to be verified.

    final List<FhirPath> grouppingPaths = query.getGroupings().stream()
        .map(parser::parse)
        .toList();

    final List<FhirPath> filterPaths = query.getFilters().stream()
        .map(parser::parse)
        .toList();

    final List<FhirPath> aggPaths = query.getAggregations().stream()
        .map(parser::parse)
        .toList();

    final List<FhirPath> contextPaths = Stream.of(grouppingPaths, filterPaths, aggPaths)
        .flatMap(List::stream)
        .toList();

    final FhirpathEvaluator fhirEvaluator = ManyFactory.fromPaths(
        query.getSubjectResource(),
        fhirContext, dataSource,
        contextPaths).create(query.getSubjectResource());

    final List<EvaluatedPath> evaluatedFilters = evalPaths(filterPaths, fhirEvaluator);

    final Optional<Column> maybeFilter = evaluatedFilters.stream()
        .map(EvaluatedPath::getColumnValue)
        .reduce(Column::and);

    final Dataset<Row> filteredDataset = maybeFilter.map(
            fhirEvaluator.createInitialDataset()::filter)
        .orElse(fhirEvaluator.createInitialDataset());

    // compute aggregation bases 
    // TODO: for now assume that the last element on the path is the aggregation function

    final List<EvaluatedPath> evaluatedGoupings = evalPaths(grouppingPaths, fhirEvaluator);

    // get the current resource
    final List<EvaluatedPath> evaluatedValues = evalPaths(List.of(new Paths.This()),
        fhirEvaluator);

    final Dataset<Row> inputDataset = filteredDataset.select(
        Stream.of(evaluatedValues, evaluatedGoupings)
            .flatMap(List::stream)
            .map(c -> c.getColumnValue().alias(randomAlias()))
            .toArray(Column[]::new)
    );

    // now we need to explode all array columns in the dataset
    final Dataset<Row> expandeDataset = Stream.of(inputDataset.schema().fields())
        .skip(1)
        .reduce(inputDataset, (dataset, field) -> {
          if (field.dataType() instanceof ArrayType) {
            return dataset.withColumn(field.name(),
                functions.explode_outer(functions.col(field.name())));
          }
          return dataset;
        }, (dataset1, dataset2) -> dataset1);

    // then groupBy the groupings and aggregate with the relevant combiner function
    // The combiner function is used to aggregate partial aggregation results from agg fhirpaths.
    // For example for `count()` the aggregation is SQL SUM(), for 

    final Column valueColumn = Stream.of(expandeDataset.columns())
        .limit(1)
        .map(expandeDataset::col)
        .findFirst().orElseThrow();

    final Column[] groupingColumns = Stream.of(expandeDataset.columns())
        .skip(1)
        .map(c -> SqlExpressions.pruneSyntheticFields(functions.col(c)).alias(c))
        .toArray(Column[]::new);

    final FhirpathEvaluator aggEvaluator = SingleFhirpathEvaluator.of(query.getSubjectResource(),
        fhirContext,
        StaticFunctionRegistry.getInstance(),
        Map.of(), dataSource);

    //then we need to group by the grouping columns and collect the resource column to a list
    final Dataset<Row> grouppedAggSource = expandeDataset.groupBy(groupingColumns)
        .agg(functions.collect_list(valueColumn).alias(query.getSubjectResource().toCode()));

    final List<EvaluatedPath> evaluatedAggs = evalPaths(aggPaths, aggEvaluator);
    final Column[] aggColumns = evaluatedAggs.stream()
        .map(c -> c.getColumnValue().alias(randomAlias()))
        .toArray(Column[]::new);

    final Dataset<Row> resultDataset = grouppedAggSource
        .select(Stream.concat(Stream.of(groupingColumns), Stream.of(aggColumns))
            .toArray(Column[]::new));

    return new ResultWithExpressions(resultDataset,
        evaluatedAggs.stream().map(p -> p.bind(filteredDataset)).toList(),
        evaluatedGoupings.stream().map(p -> p.bind(filteredDataset)).toList(),
        evaluatedFilters
    );
  }

  @Nonnull
  List<EvaluatedPath> evalPaths(@Nonnull final List<FhirPath> paths,
      @Nonnull final FhirpathEvaluator fhirEvaluator) {
    return paths.stream()
        .map(p -> EvaluatedPath.of(p, fhirEvaluator.evaluate(p)))
        .toList();
  }


  @Value
  public static class ResultWithExpressions {

    @Nonnull
    Dataset<Row> dataset;

    @Nonnull
    List<EvaluatedPath> parsedAggregations;

    @Nonnull
    List<EvaluatedPath> parsedGroupings;

    @Nonnull
    List<EvaluatedPath> parsedFilters;

  }
}
