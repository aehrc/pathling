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
import au.csiro.pathling.encoders.ValueFunctions;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.annotations.NotImplemented;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.execution.FhirpathEvaluator;
import au.csiro.pathling.fhirpath.execution.MultiFhirpathEvaluator.ManyFactory;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.fhirpath.path.Paths.EvalFunction;
import au.csiro.pathling.io.Database;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.IntStream;
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
@NotImplemented
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

    final List<Collection> evaluatedFilters = filterPaths.stream()
        .map(fhirEvaluator::evaluate)
        .toList();

    final Optional<Column> maybeFilter = evaluatedFilters.stream()
        .map(Collection::getColumnValue)
        .reduce(Column::and);

    final Dataset<Row> filteredDataset = maybeFilter.map(
            fhirEvaluator.createInitialDataset()::filter)
        .orElse(fhirEvaluator.createInitialDataset());

    // compute aggregation bases 
    // TODO: for now assume that the last element on the path is the aggregation function

    final List<Collection> evaluatedGoupings = grouppingPaths.stream()
        .map(fhirEvaluator::evaluate)
        .toList();

    final List<Collection> evaluatedAggs = aggPaths.stream()
        .map(fhirEvaluator::evaluate)
        .toList();

    final Dataset<Row> inputDataset = filteredDataset.select(
        Stream.of(evaluatedGoupings, evaluatedAggs)
            .flatMap(List::stream)
            .map(c -> c.getColumnValue().alias(randomAlias()))
            .toArray(Column[]::new)
    );

    // now we need to explode all array columns in the dataset
    final Dataset<Row> expandeDataset = Stream.of(inputDataset.schema().fields())
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

    final Column[] groupingColumms = Stream.of(expandeDataset.columns())
        .limit(evaluatedGoupings.size())
        .map(expandeDataset::col)
        .toArray(Column[]::new);

    final Column[] aggColumns = Stream.of(expandeDataset.columns())
        .skip(evaluatedGoupings.size())
        .map(expandeDataset::col)
        .toArray(Column[]::new);

    // apply the appropriate combiner functions to each of the aggregation columns
    final List<Column> aggExpr =
        IntStream.range(0, aggColumns.length)
            .mapToObj(i -> getCombiner(aggPaths.get(i)).apply(aggColumns[i]))
            .toList();

    final Dataset<Row> resultDataset = expandeDataset
        .groupBy(groupingColumms)
        .agg(aggExpr.get(0), aggExpr.subList(1, aggExpr.size()).toArray(new Column[0]));

    return new ResultWithExpressions(resultDataset, evaluatedAggs, evaluatedGoupings,
        evaluatedFilters);
  }


  @Nonnull
  static Function<Column, Column> getCombiner(@Nonnull final FhirPath path) {
    // TODO: include somehow in the definition of the function
    // TODO: include all aggregation functions
    final FhirPath tail = path.last();
    if (tail instanceof final EvalFunction evalFunction) {
      switch (evalFunction.getFunctionIdentifier()) {
        case "count", "sum" -> {
          return functions::sum;
        }
        case "first" -> {
          return functions::first;
        }
      }
    }
    // by default collect all the values to a list
    return c -> ValueFunctions.unnest(functions.collect_list(c));
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
