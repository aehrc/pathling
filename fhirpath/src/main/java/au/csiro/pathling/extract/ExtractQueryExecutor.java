package au.csiro.pathling.extract;

import static au.csiro.pathling.QueryHelpers.join;
import static au.csiro.pathling.query.ExpressionWithLabel.labelsAsStream;
import static au.csiro.pathling.utilities.Preconditions.checkArgument;
import static java.util.stream.Collectors.toList;

import au.csiro.pathling.QueryExecutor;
import au.csiro.pathling.QueryHelpers.JoinType;
import au.csiro.pathling.config.QueryConfiguration;
import au.csiro.pathling.fhirpath.AbstractPath;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.Flat;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.StringCoercible;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import ca.uhn.fhir.context.FhirContext;
import java.util.ArrayList;
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

/**
 * Builds the overall query responsible for executing an extract request.
 *
 * @author John Grimes
 */
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

    // Parse each of the column expressions.
    final List<FhirPath> parsedColumns =
        parseExpressions(parserContext, query.getColumnsAsStrings());

    // Validate and coerce the types of the columns where necessary.
    final List<FhirPath> coercedColumns =
        validateAndCoerceColumns(parsedColumns, resultType);

    // Get the dataset from the last column.
    final Dataset<Row> unfiltered = coercedColumns.get(parsedColumns.size() - 1).getDataset();

    // Trim trailing nulls from the dataset.
    final Dataset<Row> trimmed = trimTrailingNulls(inputContext.getIdColumn(), coercedColumns,
        unfiltered);

    // Apply the filters.
    final Dataset<Row> filtered;
    if (query.getFilters().isEmpty()) {
      filtered = trimmed;
    } else {
      final List<String> filters = query.getFilters();

      // Parse each of the filter expressions,
      final List<FhirPath> filterPaths = parseExpressions(parserContext, filters,
          Optional.of(trimmed));

      // Get the dataset from the last filter.
      final Dataset<Row> withFilters = filterPaths.get(filterPaths.size() - 1).getDataset();

      // Combine all the filter value columns using the and operator.
      final Optional<Column> filterConstraint = filterPaths.stream()
          .map(FhirPath::getValueColumn)
          .reduce(Column::and);

      // Filter the dataset using the constraint.
      filtered = filterConstraint.map(withFilters::filter).orElse(withFilters);
    }

    // Select the column values from the dataset, applying labelling where requested.
    final Column[] columnValues = labelColumns(
        coercedColumns.stream()
            .map(FhirPath::getValueColumn),
        labelsAsStream(query.getColumns())
    ).toArray(Column[]::new);
    final Dataset<Row> selectedDataset = filtered.select(columnValues);

    // If there is a row limit, apply it.
    return query.getLimit().isPresent()
           ? selectedDataset.limit(query.getLimit().get())
           : selectedDataset;
  }

  private List<FhirPath> validateAndCoerceColumns(
      @Nonnull final List<FhirPath> columnParseResult,
      @Nonnull final ExtractResultType resultType) {

    // Perform any necessary String coercion.
    final List<FhirPath> coerced = columnParseResult.stream()
        .map(column -> {
          if (resultType == ExtractResultType.FLAT && !(column instanceof Flat)
              && column instanceof StringCoercible) {
            // If the result type is flat and the path is string-coercible, we can coerce it.
            final StringCoercible stringCoercible = (StringCoercible) column;
            return stringCoercible.asStringPath(column.getExpression());
          } else {
            return column;
          }
        }).collect(toList());

    // Validate the final set of paths.
    for (final FhirPath column : coerced) {
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

    return coerced;
  }

  @Nonnull
  private Dataset<Row> trimTrailingNulls(final @Nonnull Column idColumn,
      @Nonnull final List<FhirPath> expressions, @Nonnull final Dataset<Row> dataset) {
    checkArgument(!expressions.isEmpty(), "At least one expression is required");

    // Get the value columns associated with non-singular paths.
    final List<Column> nonSingularColumns = expressions.stream()
        .filter(fhirPath -> !fhirPath.isSingular())
        .map(FhirPath::getValueColumn)
        .collect(toList());

    if (nonSingularColumns.isEmpty()) {
      // If there are no non-singular paths, there is nothing to do.
      return dataset;
    } else {
      // Build a condition that checks whether any of the non-singular columns are not null.
      final Column additionalCondition = nonSingularColumns.stream()
          .map(Column::isNotNull)
          .reduce(Column::or)
          .get();

      // Build a dataset that contains the distinct values of all singular columns.
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

      // Join the dataset to the filtering dataset, using a right outer join.
      return join(dataset, filteringColumns, filteringDataset, filteringColumns,
          additionalCondition, JoinType.RIGHT_OUTER);
    }
  }

}
