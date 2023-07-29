package au.csiro.pathling.views;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import ca.uhn.fhir.context.FhirContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Executes a FHIR view query.
 *
 * @author John Grimes
 */
public class FhirViewExecutor {

  @Nonnull
  private final FhirContext fhirContext;

  @Nonnull
  private final SparkSession sparkSession;

  @Nonnull
  private final DataSource dataSource;

  @Nonnull
  private final Optional<TerminologyServiceFactory> terminologyServiceFactory;

  public FhirViewExecutor(@Nonnull final FhirContext fhirContext,
      @Nonnull final SparkSession sparkSession, @Nonnull final DataSource dataSource,
      @Nonnull final Optional<TerminologyServiceFactory> terminologyServiceFactory) {
    this.fhirContext = fhirContext;
    this.sparkSession = sparkSession;
    this.dataSource = dataSource;
    this.terminologyServiceFactory = terminologyServiceFactory;
  }

  @Nonnull
  public Dataset<Row> buildQuery(@Nonnull final FhirView view) {
    // Build a new expression parser, and parse all the column expressions within the query.
    final ResourceType resourceType = ResourceType.fromCode(view.getResource());
    final ResourcePath inputContext = ResourcePath.build(fhirContext, dataSource, resourceType,
        view.getResource(), true);
    final ParserContext parserContext = new ParserContext(inputContext, fhirContext, sparkSession,
        dataSource, terminologyServiceFactory, singletonList(inputContext.getIdColumn()));

    final DatasetWithColumns select = parseSelect(view.getSelect(), parserContext,
        Collections.emptyList());
    final List<String> where = view.getWhere() == null
                               ? Collections.emptyList()
                               : view.getWhere().stream()
                                   .map(WhereClause::getExpression)
                                   .collect(toList());
    final Optional<Column> filterCondition = parseExpressions(where, parserContext).stream()
        .map(FhirPath::getValueColumn)
        .reduce(Column::and);
    return filterCondition
        .map(select.getDataset()::filter)
        .orElse(select.getDataset())
        .select(select.getColumns().toArray(new Column[0]));
  }

  @Nonnull
  private DatasetWithColumns parseSelect(@Nonnull final List<SelectClause> selectGroup,
      @Nonnull final ParserContext context, @Nonnull final List<Column> columns) {
    @Nonnull DatasetWithColumns result = new DatasetWithColumns(
        context.getInputContext().getDataset(), columns);

    for (final SelectClause select : selectGroup) {
      if (select instanceof DirectSelection) {
        final FhirPath path = parseExpression(((DirectSelection) select).getExpression(),
            context.withContextDataset(result.getDataset()));
        final List<Column> newColumns = new ArrayList<>(result.getColumns());
        newColumns.add(path.getValueColumn());
        result = new DatasetWithColumns(path.getDataset(), newColumns);

      } else if (select instanceof FromSelection) {
        final FhirPath from = parseExpression(((FromSelection) select).getFrom(),
            context.withContextDataset(result.getDataset()));
        final DatasetWithColumns nestedResult = parseSelect(((FromSelection) select).getSelect(),
            context.withContextDataset(from.getDataset()), result.getColumns());
        final List<Column> newColumns = new ArrayList<>(result.getColumns());
        newColumns.addAll(nestedResult.getColumns());
        result = new DatasetWithColumns(nestedResult.getDataset(), newColumns);

      } else if (select instanceof ForEachSelection) {
        final FhirPath forEach = parseExpression(((ForEachSelection) select).getForEach(),
            context.withContextDataset(result.getDataset()));
        final DatasetWithColumns nestedResult = parseSelect(((ForEachSelection) select).getSelect(),
            context.withContextDataset(forEach.getDataset()), result.getColumns());
        final List<Column> newColumns = new ArrayList<>(result.getColumns());
        newColumns.addAll(nestedResult.getColumns());
        result = new DatasetWithColumns(nestedResult.getDataset(), newColumns);

      } else {
        throw new IllegalStateException("Unknown select clause type: " + select.getClass());
      }
    }

    return result;
  }

  @Nonnull
  private FhirPath parseExpression(@Nonnull final String expression,
      @Nonnull final ParserContext context) {
    final Parser parser = new Parser(context);
    return parser.parse(expression);
  }

  @Nonnull
  private List<FhirPath> parseExpressions(@Nonnull final List<String> expressions,
      @Nonnull final ParserContext context) {
    return expressions.stream()
        .reduce(new ArrayList<>(), (list, expression) -> {
          final ParserContext currentContext =
              list.isEmpty()
              ? context
              : context.withContextDataset(list.get(list.size() - 1).getDataset());

          final Parser parser = new Parser(currentContext);
          list.add(parser.parse(expression));
          return list;

        }, (list1, list2) -> {
          list1.addAll(list2);
          return list1;
        });
  }

}
