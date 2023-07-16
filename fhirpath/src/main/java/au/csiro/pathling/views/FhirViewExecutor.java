package au.csiro.pathling.views;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.apache.spark.sql.functions.flatten;

import au.csiro.pathling.QueryExecutor;
import au.csiro.pathling.config.QueryConfiguration;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.FhirPathAndContext;
import au.csiro.pathling.fhirpath.FhirValue;
import au.csiro.pathling.fhirpath.NonLiteralPath;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.fhirpath.parser.UnnestBehaviour;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import ca.uhn.fhir.context.FhirContext;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Executes a FHIR view query.
 *
 * @author John Grimes
 */
public class FhirViewExecutor extends QueryExecutor {

  private static Map<WhenMany, UnnestBehaviour> WHEN_MANY_UNNEST_MAP = new HashMap<>();

  static {
    WHEN_MANY_UNNEST_MAP.put(WhenMany.UNNEST, UnnestBehaviour.UNNEST);
    WHEN_MANY_UNNEST_MAP.put(WhenMany.ERROR, UnnestBehaviour.ERROR);
    WHEN_MANY_UNNEST_MAP.put(WhenMany.ARRAY, UnnestBehaviour.NOOP);
  }


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
      final UnnestBehaviour unnestBehaviour = WHEN_MANY_UNNEST_MAP.get(variable.getWhenMany());

      // Create a copy of the parser context that uses the specified unnest behaviour.
      final ParserContext variableContext = parserContext
          .withUnnestBehaviour(unnestBehaviour);

      final Parser parser = new Parser(variableContext);
      FhirPath result = parser.parse(variable.getExpression())
          .withExpression("%" + variable.getName());

      // If the variable has a `whenMany` value of `array`, we need to flatten the result to remove 
      // accumulated array nesting from the underlying structure.
      if (variable.getWhenMany() == WhenMany.ARRAY && result instanceof NonLiteralPath
          && result instanceof FhirValue) {
        final NonLiteralPath nonLiteralResult = (NonLiteralPath) result;
        final Column flattenedValue = flatten(nonLiteralResult.getValueColumn());
        result = nonLiteralResult.copy(nonLiteralResult.getExpression(),
            nonLiteralResult.getDataset(), nonLiteralResult.getIdColumn(), flattenedValue,
            nonLiteralResult.getOrderingColumn(), nonLiteralResult.isSingular(),
            nonLiteralResult.getThisColumn());
      }

      // Add the variable path and its context to the parser context. This ensures that previously 
      // declared variables can be referenced in later ones.
      parserContext.getVariables().put(variable.getName(),
          new FhirPathAndContext(result, variableContext));
    }

    // Parse the column expressions.
    final ParserContext columnContext = buildParserContext(inputContext,
        singletonList(inputContext.getIdColumn())).withUnnestBehaviour(UnnestBehaviour.NOOP);
    columnContext.getVariables().putAll(parserContext.getVariables());
    final List<String> columnExpressions = view.getColumns().stream()
        .map(NamedExpression::getExpression)
        .collect(toList());
    // final List<FhirPath> columnParseResult =
    //     parseExpressions(columnContext, columnExpressions);
    final List<FhirPath> columnParseResult = new ArrayList<>();
    final List<FhirPath> columnPaths = columnParseResult;

    // Join the column expressions together.
    final Dataset<Row> unfilteredDataset = columnPaths.get(columnPaths.size() - 1).getDataset();

    // Filter the dataset.
    final Dataset<Row> filteredDataset = filterDataset(inputContext, view.getFilters(),
        unfilteredDataset, Column::and);

    // Select the column values.
    final Column idColumn = inputContext.getIdColumn();
    final Column[] columnValues = labelColumns(
        columnPaths.stream().map(FhirPath::getValueColumn),
        view.getColumns().stream().map(NamedExpression::getName).map(Optional::of)
    ).toArray(Column[]::new);
    return filteredDataset.select(columnValues)
        .filter(idColumn.isNotNull());
  }

}
