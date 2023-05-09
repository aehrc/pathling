package au.csiro.pathling.views;

import static java.util.stream.Collectors.toList;

import au.csiro.pathling.QueryExecutor;
import au.csiro.pathling.config.QueryConfiguration;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.Materializable;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import ca.uhn.fhir.context.FhirContext;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
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

    // The context of evaluation is a single resource.
    final ParserContext parserContext = buildParserContext(inputContext,
        Collections.singletonList(inputContext.getIdColumn()));
    final List<String> columnExpressions = view.getColumns().stream()
        .map(NamedExpression::getExpression)
        .collect(toList());
    final List<FhirPathAndContext> columnParseResult =
        parseMaterializableExpressions(parserContext, columnExpressions, "Column");
    final List<FhirPath> columnPaths = columnParseResult.stream()
        .map(FhirPathAndContext::getFhirPath)
        .collect(Collectors.toUnmodifiableList());
    final Dataset<Row> unfilteredDataset = joinExpressionsWithoutCorrelation(inputContext,
        columnPaths,
        inputContext.getIdColumn());
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


}
