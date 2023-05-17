package au.csiro.pathling.views;

import static java.util.stream.Collectors.joining;
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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
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

    // Parse the variable expressions.
    ParserContext variableContext = buildParserContext(inputContext,
        Collections.singletonList(inputContext.getIdColumn()));
    for (final VariableExpression variable : view.getVariables()) {
      final UnnestBehaviour unnestBehaviour = variable.getWhenMany() == WhenMany.UNNEST
                                              ? UnnestBehaviour.UNNEST
                                              : UnnestBehaviour.NOOP;
      variableContext = variableContext.withUnnestBehaviour(unnestBehaviour);
      final Parser parser = new Parser(variableContext);
      final FhirPath result = parser.parse(variable.getExpression());
      variableContext.getVariables().put(variable.getName(), new FhirPathAndContext(result,
          variableContext));
    }

    // Parse the column expressions.
    final ParserContext columnContext = variableContext.withUnnestBehaviour(UnnestBehaviour.NOOP);
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
      throw new IllegalArgumentException("No columns to join");

    } else if (columnsAndContexts.size() == 1) {
      final FhirPathAndContext fhirPathAndContext = columnsAndContexts.iterator().next();
      return fhirPathAndContext.getFhirPath().getDataset();
    }

    final List<FhirPathAndContext> sorted = columnsAndContexts.stream()
        .sorted((a, b) -> {
          final String sortStringA = a.getContext().getNodeIdColumns().values().stream()
              .map(Column::toString)
              .collect(joining(""));
          final String sortStringB = b.getContext().getNodeIdColumns().values().stream()
              .map(Column::toString)
              .collect(joining(""));
          return sortStringA.compareTo(sortStringB) * -1;
        })
        .collect(toList());

    FhirPathAndContext left = sorted.get(0);
    Dataset<Row> result = sorted.get(0).getFhirPath().getDataset();

    for (final FhirPathAndContext right : sorted) {
      final Set<Column> leftJoinColumns = new HashSet<>();
      final Set<Column> rightJoinColumns = new HashSet<>();
      leftJoinColumns.add(left.getFhirPath().getIdColumn());
      rightJoinColumns.add(right.getFhirPath().getIdColumn());

      final Set<Column> commonNodeIds = new HashSet<>(left.getContext().getNodeIdColumns()
          .values());
      commonNodeIds.retainAll(right.getContext().getNodeIdColumns().values());
      leftJoinColumns.addAll(commonNodeIds);
      rightJoinColumns.addAll(commonNodeIds);

      result = QueryHelpers.join(result, new ArrayList<>(leftJoinColumns),
          right.getFhirPath().getDataset(), new ArrayList<>(rightJoinColumns), JoinType.LEFT_OUTER);
      left = right;
    }

    return result;
  }

}
