package au.csiro.pathling.extract;

import static au.csiro.pathling.QueryHelpers.join;
import static au.csiro.pathling.query.ExpressionWithLabel.labelsAsStream;
import static au.csiro.pathling.utilities.Preconditions.checkArgument;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import au.csiro.pathling.QueryExecutor;
import au.csiro.pathling.QueryHelpers;
import au.csiro.pathling.QueryHelpers.JoinType;
import au.csiro.pathling.config.QueryConfiguration;
import au.csiro.pathling.fhirpath.AbstractPath;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.FhirPathAndContext;
import au.csiro.pathling.fhirpath.Flat;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.StringCoercible;
import au.csiro.pathling.fhirpath.TopologicalExpressionSorter;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import ca.uhn.fhir.context.FhirContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

@Slf4j
public class ExtractQueryExecutor extends QueryExecutor {

  public ExtractQueryExecutor(@Nonnull final QueryConfiguration configuration,
      @Nonnull final FhirContext fhirContext, @Nonnull final SparkSession sparkSession,
      @Nonnull final DataSource dataSource,
      @Nonnull final Optional<TerminologyServiceFactory> terminologyServiceFactory) {
    super(configuration, fhirContext, sparkSession, dataSource, terminologyServiceFactory);
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
    return buildQuery(query, ExtractResultType.UNCONSTRAINED);
  }

  /**
   * Builds up the query for an extract request.
   *
   * @param query an {@link ExtractRequest}
   * @param resultType the {@link ExtractResultType} that will be required
   * @return an uncollected {@link Dataset}
   */
  @SuppressWarnings("WeakerAccess")
  @Nonnull
  public Dataset<Row> buildQuery(@Nonnull final ExtractRequest query,
      @Nonnull final ExtractResultType resultType) {

    // The context of evaluation is a single resource.
    final ResourcePath inputContext = ResourcePath
        .build(getFhirContext(), getDataSource(), query.getSubjectResource(),
            query.getSubjectResource().toCode(), true);
    final ParserContext parserContext = buildParserContext(inputContext,
        Collections.singletonList(inputContext.getIdColumn()));
    final List<FhirPathAndContext> parsedColumns =
        parseExpressions(parserContext, query.getColumnsAsStrings());
    // final List<FhirPathAndContext> coercedColumns =
    //     validateAndCoerceColumns(parsedColumns, resultType);
    // final Dataset<Row> queryDataset = joinAllColumns(coercedColumns);

    // Trim trailing nulls.
    // final Dataset<Row> trimmedDataset = trimTrailingNulls(
    //     parserContext.getInputContext().getIdColumn(), coercedColumns, queryDataset);
    final Dataset<Row> trimmedDataset = parsedColumns.get(parsedColumns.size() - 1).getFhirPath()
        .getDataset();

    // Apply the filters.
    final List<String> filters = query.getFilters();
    final Dataset<Row> filteredDataset = filterDataset(inputContext, filters, trimmedDataset,
        Column::and);

    // Select the column values.
    final Column idColumn = inputContext.getIdColumn();
    final Column[] columnValues = labelColumns(
        parsedColumns.stream()
            .map(FhirPathAndContext::getFhirPath)
            .map(FhirPath::getValueColumn),
        labelsAsStream(query.getColumns())
    ).toArray(Column[]::new);
    final Dataset<Row> selectedDataset = filteredDataset.select(columnValues)
        .filter(idColumn.isNotNull());

    // If there is a limit, apply it.
    return query.getLimit().isPresent()
           ? selectedDataset.limit(query.getLimit().get())
           : selectedDataset;
  }

  private List<FhirPathAndContext> validateAndCoerceColumns(
      @Nonnull final List<FhirPathAndContext> parsedColumns,
      @Nonnull final ExtractResultType resultType) {

    // Perform any necessary String coercion.
    final List<FhirPathAndContext> coercedColumns = parsedColumns.stream()
        .map(columnAndContext -> {
          final FhirPath column = columnAndContext.getFhirPath();
          if (resultType == ExtractResultType.FLAT && !(column instanceof Flat)
              && column instanceof StringCoercible) {
            // If the result type is flat and the path is string-coercible, we can coerce it.
            final StringCoercible stringCoercible = (StringCoercible) column;
            return new FhirPathAndContext(stringCoercible.asStringPath(column.getExpression()),
                columnAndContext.getContext());
          } else {
            return new FhirPathAndContext(column, columnAndContext.getContext());
          }
        }).collect(toList());

    // Validate the final set of paths.
    for (final FhirPathAndContext columnAndContext : coercedColumns) {
      final FhirPath column = columnAndContext.getFhirPath();
      final boolean condition;
      if (resultType == ExtractResultType.FLAT) {
        // In flat mode, only flat columns are allowed.
        condition = column instanceof Flat;
      } else {
        // Otherwise, a column can be of any type, as long as it has not been specifically flagged 
        // as being abstract, e.g. an UntypedResourcePath.
        condition = !(column instanceof AbstractPath);
      }
      checkArgument(condition, "Column is not of a supported type: " + column.getExpression());
    }

    return coercedColumns;
  }

  @Nonnull
  private Dataset<Row> joinAllColumns(
      @Nonnull final Collection<FhirPathAndContext> columnsAndContexts) {
    if (columnsAndContexts.isEmpty()) {
      // If there are no columns, throw an error.
      throw new IllegalArgumentException("No columns to join");

    } else if (columnsAndContexts.size() == 1) {
      // If there is only one column, skip joining and return its dataset.
      final FhirPathAndContext fhirPathAndContext = columnsAndContexts.iterator().next();
      return fhirPathAndContext.getFhirPath().getDataset();
    }

    // Sort the columns by the nodes encountered while parsing. This ensures that we join them 
    // together in order from the general to the specific.
    final List<FhirPathAndContext> sorted = columnsAndContexts.stream()
        .sorted(new TopologicalExpressionSorter())
        .collect(toList());

    // Start with the first column and its unjoined dataset.
    FhirPathAndContext left = sorted.get(0);
    Dataset<Row> result = left.getFhirPath().getDataset();

    // Move through the list of columns, joining each one to the result of the previous join.
    for (final FhirPathAndContext right : sorted.subList(1, sorted.size())) {
      final List<Column> leftJoinColumns = new ArrayList<>();
      final List<Column> rightJoinColumns = new ArrayList<>();

      // The join column always includes the resource ID.
      leftJoinColumns.add(left.getFhirPath().getIdColumn());
      rightJoinColumns.add(right.getFhirPath().getIdColumn());

      // Add the intersection of the nodes present in both the left and right column contexts.
      final List<Object> commonNodes = new ArrayList<>(
          left.getContext().getNodeIdColumns().keySet());
      commonNodes.retainAll(right.getContext().getNodeIdColumns().keySet());
      final FhirPathAndContext finalLeft = left;
      leftJoinColumns.addAll(commonNodes.stream()
          .flatMap(key -> finalLeft.getContext().getNodeIdColumns().get(key).stream())
          .collect(toSet()));
      rightJoinColumns.addAll(commonNodes.stream()
          .flatMap(key -> right.getContext().getNodeIdColumns().get(key).stream())
          .collect(toSet()));

      // Use a left outer join, so that we don't lose rows that don't have a value for the right
      // column.
      result = QueryHelpers.join(result, leftJoinColumns, right.getFhirPath().getDataset(),
          rightJoinColumns, JoinType.LEFT_OUTER);

      // The result of the join becomes the left side of the next join.
      left = right;
    }

    return result;
  }

  // @Nonnull
  // private Dataset<Row> filterInvalidRowCombinations(@Nonnull final Dataset<Row> dataset, @Nonnull final Column idColumn, )

  @Nonnull
  private Dataset<Row> trimTrailingNulls(final @Nonnull Column idColumn,
      @Nonnull final List<FhirPath> expressions, @Nonnull final Dataset<Row> dataset) {
    checkArgument(!expressions.isEmpty(), "At least one expression is required");

    final Column[] nonSingularColumns = expressions.stream()
        .filter(fhirPath -> !fhirPath.isSingular())
        .map(FhirPath::getValueColumn)
        .toArray(Column[]::new);

    if (nonSingularColumns.length == 0) {
      return dataset;
    } else {
      final Column additionalCondition = Arrays.stream(nonSingularColumns)
          .map(Column::isNotNull)
          .reduce(Column::or)
          .get();
      final List<Column> filteringColumns = new ArrayList<>();
      filteringColumns.add(idColumn);
      final List<Column> singularColumns = expressions.stream()
          .filter(FhirPath::isSingular)
          .map(FhirPath::getValueColumn)
          .collect(Collectors.toList());
      filteringColumns.addAll(singularColumns);
      final Dataset<Row> filteringDataset = dataset
          .select(filteringColumns.toArray(new Column[0]))
          .distinct();
      return join(dataset, filteringColumns, filteringDataset, filteringColumns,
          additionalCondition, JoinType.RIGHT_OUTER);
    }
  }

}
