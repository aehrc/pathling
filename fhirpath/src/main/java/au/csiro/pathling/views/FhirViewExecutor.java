package au.csiro.pathling.views;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toUnmodifiableList;

import au.csiro.pathling.QueryExecutor;
import au.csiro.pathling.QueryHelpers;
import au.csiro.pathling.QueryHelpers.JoinType;
import au.csiro.pathling.config.QueryConfiguration;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.FhirPathAndContext;
import au.csiro.pathling.fhirpath.Materializable;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.fhirpath.parser.UnnestBehaviour;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import ca.uhn.fhir.context.FhirContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class FhirViewExecutor extends QueryExecutor {


  public FhirViewExecutor(
      @Nonnull final QueryConfiguration configuration,
      @Nonnull final FhirContext fhirContext,
      @Nonnull final SparkSession sparkSession,
      @Nonnull final DataSource dataSource,
      @Nonnull final Optional<TerminologyServiceFactory> terminologyServiceFactory) {
    super(configuration, fhirContext, sparkSession, dataSource, terminologyServiceFactory);
  }

  @Nonnull
  public Dataset<Row> buildQuery(@Nonnull final FhirView view) {
    // Build a new expression parser, and parse all the column expressions within the query.
    final ResourcePath inputContext = ResourcePath
        .build(getFhirContext(), getDataSource(), view.getResource(),
            view.getResource().toCode(), true);
    final ParserContext parserContext = buildParserContext(inputContext,
        singletonList(inputContext.getIdColumn()));

    // Parse the variable expressions.
    for (final VariableExpression variable : view.getVariables()) {
      final UnnestBehaviour unnestBehaviour = variable.getWhenMany() == WhenMany.UNNEST
                                              ? UnnestBehaviour.UNNEST
                                              : UnnestBehaviour.NOOP;

      // Create a copy of the parser context that uses the specified unnest behaviour.
      final ParserContext variableContext = parserContext
          .withUnnestBehaviour(unnestBehaviour)
          .unsetNodeIds();

      final Parser parser = new Parser(variableContext);
      final FhirPath result = parser.parse(variable.getExpression())
          .withExpression("%" + variable.getName());

      // Add the variable path and its context to the parser context. This ensures that previously 
      // declared variables can be referenced in later ones.
      parserContext.getVariables().put(variable.getName(), new FhirPathAndContext(result,
          variableContext));
    }

    // Parse the column expressions.
    final ParserContext columnContext = buildParserContext(inputContext,
        singletonList(inputContext.getIdColumn())).withUnnestBehaviour(UnnestBehaviour.NOOP);
    columnContext.getVariables().putAll(parserContext.getVariables());
    final List<String> columnExpressions = view.getColumns().stream()
        .map(NamedExpression::getExpression)
        .collect(toList());
    final List<FhirPathAndContext> columnParseResult =
        parseExpressions(columnContext, columnExpressions, "Column", true);
    final List<FhirPath> columnPaths = columnParseResult.stream()
        .map(FhirPathAndContext::getFhirPath)
        .collect(toUnmodifiableList());

    // Join the column expressions together.
    final Dataset<Row> unfilteredDataset = joinAllColumns(columnParseResult);

    // Filter the dataset.
    final Dataset<Row> filteredDataset = filterDataset(inputContext, view.getFilters(),
        unfilteredDataset, Column::and);

    // Select the column values.
    final Column idColumn = inputContext.getIdColumn();
    final Column[] columnValues = labelColumns(
        columnPaths.stream().map(path -> ((Materializable<?>) path).getExtractableColumn()),
        view.getColumns().stream().map(NamedExpression::getName).map(Optional::of)
    ).toArray(Column[]::new);
    return filteredDataset.select(columnValues)
        .filter(idColumn.isNotNull());
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
        .sorted((a, b) -> {
          final List<String> nodesA = a.getContext().getNodeIdColumns().keySet().stream()
              .sorted()
              .collect(toList());
          final List<String> nodesB = b.getContext().getNodeIdColumns().keySet().stream()
              .sorted()
              .collect(toList());
          final String sortStringA = String.join("|", nodesA);
          final String sortStringB = String.join("|", nodesB);
          return sortStringA.compareTo(sortStringB);
        })
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
      final List<String> commonNodes = new ArrayList<>(
          left.getContext().getNodeIdColumns().keySet());
      commonNodes.retainAll(right.getContext().getNodeIdColumns().keySet());
      final FhirPathAndContext finalLeft = left;
      leftJoinColumns.addAll(commonNodes.stream()
          .map(key -> finalLeft.getContext().getNodeIdColumns().get(key))
          .collect(toList()));
      rightJoinColumns.addAll(commonNodes.stream()
          .map(key -> right.getContext().getNodeIdColumns().get(key))
          .collect(toList()));

      // Use a left outer join, so that we don't lose rows that don't have a value for the right
      // column.
      result = QueryHelpers.join(result, leftJoinColumns, right.getFhirPath().getDataset(),
          rightJoinColumns, JoinType.LEFT_OUTER);

      // The result of the join becomes the left side of the next join.
      left = right;
    }

    return result;
  }

}
