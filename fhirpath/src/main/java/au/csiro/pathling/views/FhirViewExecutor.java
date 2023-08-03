package au.csiro.pathling.views;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.parser.ConstantReplacer;
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
    // Get the constants from the query, and build a replacer.
    final Optional<ConstantReplacer> constantReplacer = Optional.ofNullable(view.getConstants())
        .map(c -> c.stream()
            .collect(toMap(ConstantDeclaration::getName, ConstantDeclaration::getValue)))
        .map(ConstantReplacer::new);

    // Build a new expression parser, and parse all the column expressions within the query.
    final ResourceType resourceType = ResourceType.fromCode(view.getResource());
    final ResourcePath inputContext = ResourcePath.build(fhirContext, dataSource, resourceType,
        view.getResource(), true);
    final ParserContext parserContext = new ParserContext(inputContext, fhirContext, sparkSession,
        dataSource, terminologyServiceFactory, singletonList(inputContext.getIdColumn()),
        constantReplacer);

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
    @Nonnull ParserContext currentContext = context;

    for (final SelectClause select : selectGroup) {
      if (select instanceof DirectSelection) {
        result = directSelection(currentContext, (DirectSelection) select, result);

      } else if (select instanceof FromSelection) {
        result = nestedSelection(currentContext, (FromSelection) select, result, false);

      } else if (select instanceof ForEachSelection) {
        result = nestedSelection(currentContext, (ForEachSelection) select, result, true);

      } else {
        throw new IllegalStateException("Unknown select clause type: " + select.getClass());
      }
      currentContext = currentContext.withContextDataset(result.getContext().getDataset());
      currentContext.unsetThisContext();
    }

    return result;
  }

  @Nonnull
  private ContextAndSelection directSelection(final @Nonnull ParserContext context,
      @Nonnull final DirectSelection select, @Nonnull final ContextAndSelection result) {
    final FhirPath path = parseExpression(select.getExpression(),
        context.withInputContext(result.getContext()));
    final List<Column> newColumns = new ArrayList<>(result.getSelection());
    newColumns.add(path.getValueColumn().alias(select.getName()));
    return new ContextAndSelection(context.getInputContext().withDataset(path.getDataset()),
        newColumns);
  }

  @Nonnull
  private ContextAndSelection nestedSelection(final @Nonnull ParserContext context,
      @Nonnull final NestedSelectClause select, @Nonnull final ContextAndSelection result,
      final boolean unnest) {
    final FhirPath from = parseExpression(select.getExpression(),
        context.withInputContext(result.getContext()));
    final FhirPath nextInputContext = unnest
                                      ? from.unnest()
                                      : from;
    final ParserContext nextContext = context.withInputContext(nextInputContext);
    nextContext.setThisContext(nextInputContext);
    final ContextAndSelection nestedResult = parseSelect(select.getSelect(), nextContext,
        result.getSelection());
    return new ContextAndSelection(context.withContextDataset(nestedResult.getContext()
        .getDataset()).getInputContext(), nestedResult.getSelection());
  }

  @Nonnull
  private FhirPath parseExpression(@Nonnull final String expression,
      @Nonnull final ParserContext context) {
    final Parser parser = new Parser(context);
    final String updatedExpression = context.getConstantReplacer()
        .map(replacer -> replacer.execute(expression))
        .orElse(expression);
    return parser.parse(updatedExpression);
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

          list.add(parseExpression(expression, currentContext));
          return list;

        }, (list1, list2) -> {
          list1.addAll(list2);
          return list1;
        });
  }

}
