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

    final ContextAndSelection select = parseSelect(view.getSelect(), parserContext,
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
        .map(select.getContext().getDataset()::filter)
        .orElse(select.getContext().getDataset())
        .select(select.getSelection().toArray(new Column[0]));
  }

  @Nonnull
  private ContextAndSelection parseSelect(@Nonnull final List<SelectClause> selectGroup,
      @Nonnull final ParserContext context, @Nonnull final List<Column> selection) {
    @Nonnull ContextAndSelection result = new ContextAndSelection(
        context.getInputContext(), selection);

    for (final SelectClause select : selectGroup) {
      if (select instanceof DirectSelection) {
        final DirectSelection directSelection = (DirectSelection) select;
        final FhirPath path = parseExpression(directSelection.getExpression(),
            context.withInputContext(result.getContext()));
        final List<Column> newColumns = new ArrayList<>(result.getSelection());
        newColumns.add(path.getValueColumn().alias(directSelection.getName()));
        result = new ContextAndSelection(context.getInputContext().withDataset(path.getDataset()),
            newColumns);

      } else if (select instanceof FromSelection) {
        final FromSelection fromSelection = (FromSelection) select;
        final FhirPath from = parseExpression(fromSelection.getFrom(),
            context.withInputContext(result.getContext()));
        final ContextAndSelection nestedResult = parseSelect(fromSelection.getSelect(),
            context.withInputContext(from), result.getSelection());
        result = new ContextAndSelection(nestedResult.getContext(), nestedResult.getSelection());

      } else if (select instanceof ForEachSelection) {
        final FhirPath forEach = parseExpression(((ForEachSelection) select).getForEach(),
            context.withInputContext(result.getContext()));
        final ContextAndSelection nestedResult = parseSelect(
            ((ForEachSelection) select).getSelect(), context.withInputContext(forEach),
            result.getSelection());
        result = new ContextAndSelection(nestedResult.getContext(), nestedResult.getSelection());

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
