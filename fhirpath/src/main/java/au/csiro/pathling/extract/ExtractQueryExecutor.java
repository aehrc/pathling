package au.csiro.pathling.extract;

import static au.csiro.pathling.utilities.Strings.randomAlias;
import static java.util.stream.Collectors.toList;

import au.csiro.pathling.QueryExecutor;
import au.csiro.pathling.config.QueryConfiguration;
import au.csiro.pathling.extract.ImplicitUnnester.FhirPathWithTag;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.execution.FhirpathEvaluators.MultiEvaluatorFactory;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.query.ExpressionWithLabel;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.view.ColumnSelection;
import au.csiro.pathling.view.ExecutionContext;
import au.csiro.pathling.view.GroupingSelection;
import au.csiro.pathling.view.Projection;
import au.csiro.pathling.view.ProjectionClause;
import au.csiro.pathling.view.RequestedColumn;
import au.csiro.pathling.view.UnnestingSelection;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

/**
 * Builds the overall query responsible for executing an extract request.
 *
 * @author John Grimes
 * @see <a href="https://pathling.csiro.au/docs/libraries/fhirpath-query#extract">Pathling
 * documentation - extract</a>
 */
@Slf4j
public class ExtractQueryExecutor extends QueryExecutor {

  @Value(staticConstructor = "of")
  private static class ProjectionWithColumnAliases {

    @Nonnull
    Projection projection;
    @Nonnull
    List<String> columnAliases;
  }


  @Nonnull
  private final FhirContext fhirContext;

  @Nonnull
  private final SparkSession sparkSession;

  @Nonnull
  private final DataSource dataSource;

  @Nonnull
  private final Parser parser;

  /**
   * @param configuration A {@link QueryConfiguration} that controls the behaviour of the query
   * @param fhirContext A {@link FhirContext} for querying FHIR definitions
   * @param sparkSession A {@link SparkSession} for executing the query
   * @param dataSource A {@link DataSource} for reading data
   * @param terminologyServiceFactory An optional {@link TerminologyServiceFactory} for resolving
   * terminology queries
   */
  public ExtractQueryExecutor(@Nonnull final QueryConfiguration configuration,
      @Nonnull final FhirContext fhirContext, @Nonnull final SparkSession sparkSession,
      @Nonnull final DataSource dataSource,
      @Nonnull final Optional<TerminologyServiceFactory> terminologyServiceFactory) {
    super(configuration, fhirContext, sparkSession, dataSource, terminologyServiceFactory);
    this.fhirContext = fhirContext;
    this.sparkSession = sparkSession;
    this.dataSource = dataSource;
    this.parser = new Parser();
  }

  /**
   * Builds up the query for an extract request.
   *
   * @param query an {@link ExtractRequest}
   * @return an uncollected {@link Dataset}
   */
  @SuppressWarnings("WeakerAccess")
  @Nonnull
  public Dataset<Row> buildQuery(@Nonnull final ExtractRequest query) {
    return buildQuery(query, ProjectionConstraint.UNCONSTRAINED);
  }

  /**
   * Builds up the query for an extract request, with a constraint.
   *
   * @param query an {@link ExtractRequest}
   * @param constraint a {@link ProjectionConstraint}
   * @return an uncollected {@link Dataset}
   */
  @Nonnull
  public Dataset<Row> buildQuery(@Nonnull final ExtractRequest query,
      @Nonnull final ProjectionConstraint constraint) {

    // TODO: optimize so that the parsing happens only one time
    // all paths
    final List<FhirPath> contextPaths =
        Stream.concat(query.getColumns().stream()
                .map(expression -> parser.parse(expression.getExpression())),
            query.getFilters().stream().map(parser::parse)
        ).toList();

    final ExecutionContext executionContext = new ExecutionContext(sparkSession,
        MultiEvaluatorFactory.fromPaths(
            query.getSubjectResource(),
            fhirContext, dataSource,
            contextPaths
        ));

    // Build a Projection from the ExtractRequest.
    final ProjectionWithColumnAliases projectionWithAliases = buildProjection(query, constraint);
    final Projection projection = projectionWithAliases.getProjection();
    log.debug("Executing projection:\n {}", projection.toTreeString());

    // Execute the Projection to get the result dataset.
    Dataset<Row> result = projection.execute(executionContext)
        // reorder the result to match the oder of initial extract expressions
        .select(projectionWithAliases.getColumnAliases().stream().map(functions::col)
            .toArray(Column[]::new));

    // Rename each column in the result to match the requested column names.
    final List<String> requestedColumnNames = query.getColumns().stream()
        .map(ExpressionWithLabel::getLabel)
        .toList();
    final List<String> resultColumnAliases = projectionWithAliases.getColumnAliases();

    for (int i = 0; i < requestedColumnNames.size(); i++) {
      final String requestedName = requestedColumnNames.get(i);
      final String resultName = resultColumnAliases.get(i);
      result = requestedName != null
               ? result.withColumnRenamed(resultName, requestedName)
               : result;
    }

    final Dataset<Row> finalResult = result;
    return query.getLimit().map(finalResult::limit).orElse(finalResult);
  }

  @Nonnull
  private ProjectionWithColumnAliases buildProjection(@Nonnull final ExtractRequest query,
      final ProjectionConstraint constraint) {
    // Parse each column in the query into a FhirPath object.

    // Mark each path with a random alias so that we can correlate the results
    // with the original query.
    final List<FhirPathWithTag> columns = query.getColumns().stream()
        .map(expression -> FhirPathWithTag.of(parser.parse(expression.getExpression()),
            randomAlias()))
        .collect(toList());

    // Build the column selection.
    final ProjectionClause selection = buildSelectClause(columns);

    // Build the filters.
    final Optional<ProjectionClause> filters = buildFilterClause(query.getFilters());

    // Return the final Projection object.
    return ProjectionWithColumnAliases.of(
        new Projection(query.getSubjectResource(), Collections.emptyList(), selection,
            filters, constraint),
        columns.stream().map(FhirPathWithTag::getRequiredTag).toList());
  }

  static ProjectionClause fromTree(@Nonnull final Tree<FhirPathWithTag> tree) {
    if (tree instanceof Tree.Leaf<FhirPathWithTag> leaf) {
      // for each leaf we create column selection of it's value
      // the implicit unnesting of the collection nodes is not explicitly represented in the tree
      return new ColumnSelection(
          List.of(
              new RequestedColumn(leaf.getValue().getPath(), leaf.getValue().getRequiredTag(),
                  false, Optional.empty())
          )
      );
    } else if (tree instanceof Tree.Node<FhirPathWithTag> node) {
      // each node represents an unnesting selection of its children
      return new UnnestingSelection(node.getValue().getPath(), node.getChildren().stream()
          .map(ExtractQueryExecutor::fromTree)
          .collect(toList()), true);
    } else {
      throw new IllegalArgumentException("Unknown tree type: " + tree.getClass());
    }
  }

  @Nonnull
  static ProjectionClause buildSelectClause(@Nonnull final List<FhirPathWithTag> paths) {
    if (paths.isEmpty()) {
      throw new IllegalArgumentException("Empty column list");
    }
    final Tree<FhirPathWithTag> unnestingTree = new ImplicitUnnester().unnestPaths(paths);
    log.debug("Unnested tree:\n{}",
        unnestingTree.map(FhirPathWithTag::toExpression).toTreeString());
    // this should be a selection for the resource with the unnesting tree
    final UnnestingSelection resourceSelection = (UnnestingSelection) fromTree(unnestingTree);
    final List<ProjectionClause> selects = resourceSelection.getComponents();
    // If there is more than one select, return a GroupingSelection.
    // Otherwise, return the single select by itself.
    return selects.size() > 1
           ? new GroupingSelection(selects)
           : selects.get(0);
  }

  @Nonnull
  private Optional<ProjectionClause> buildFilterClause(@Nonnull final List<String> filters) {
    // If there are no filters, return an empty result.
    if (filters.isEmpty()) {
      return Optional.empty();
    }

    // Parse each filter into a FhirPath object.
    final List<RequestedColumn> selections = filters.stream()
        .map(parser::parse)
        .map(expression ->
            new RequestedColumn(expression, randomAlias(), false, Optional.empty()))
        .collect(toList());

    // Return a ColumnSelection object containing all the filters.
    return Optional.of(new ColumnSelection(selections));
  }

}
